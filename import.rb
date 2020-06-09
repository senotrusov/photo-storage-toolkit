require 'concurrent'
require 'date'
require 'digest'
require 'exifr/jpeg'
require 'fileutils'
require 'open3'
require 'pathname'
require 'sqlite3'

def measure_time
  start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  yield
  elapsed = (Process.clock_gettime(Process::CLOCK_MONOTONIC) - start).to_i
  puts "Elapsed time: #{elapsed / 3600}h #{(elapsed % 3600) / 60}m #{elapsed % 60}s" if VERBOSE
end

def ensure_dir_exists path
  if File.exist?(path)
    unless File.directory?(path)
      STDERR.puts (error_string = "#{path} is not a directory")
      raise error_string
    end
  else
    FileUtils.mkdir_p path
  end
end

def remove_empty_dirs path
  Dir.glob(path + '**/').reverse[0..-2].each do |dir|
    if Dir.entries(dir).size == 2
      Dir.rmdir dir
    end
  end
end


class DigestDatabase
  def initialize path
    @db = SQLite3::Database.new((path + 'digests.sqlite').to_s)

    unless @db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name = 'digests'").length == 1
      @db.execute 'PRAGMA encoding = "UTF-8";'
      @db.execute "CREATE TABLE digests (id INTEGER PRIMARY KEY, filename TEXT, digest TEXT);"
      @db.execute "CREATE INDEX digests_filename ON digests(filename);"
      @db.execute "CREATE UNIQUE INDEX digests_digest ON digests(digest);"
    end

    @insert = @db.prepare("INSERT INTO digests (filename, digest) VALUES (?, ?)")
    @find = @db.prepare("SELECT count(*) FROM digests WHERE digest = ?")

    @mutex = Mutex.new
  end

  def insert filename, digest
    @mutex.synchronize do
      @insert.execute! filename.to_s, digest
    end
  end

  def exist? digest
    @mutex.synchronize do
      result = @find.execute! digest
      result[0][0] != 0
    end
  end
end

class Importer
  JPEG_EXTENSIONS = ['.jpg', '.jpeg']
  PNG_EXTENSIONS = ['.png']
  VIDEO_EXTENSIONS = ['.mov', '.mp4', '.flv']

  ALL_IMAGE_EXTENSIONS = JPEG_EXTENSIONS + PNG_EXTENSIONS
  MEDIA_EXTENSIONS = ALL_IMAGE_EXTENSIONS + VIDEO_EXTENSIONS

  def initialize incoming_path, storage_path
    @incoming_path = incoming_path
    @storage_path = storage_path

    ensure_dir_exists @incoming_path
    ensure_dir_exists @storage_path

    @digest_database = DigestDatabase.new storage_path
    @pool = Concurrent::FixedThreadPool.new(CONCURRENCY)
    @storage_mutex = Mutex.new
  end

  # TODO: Promises and progress bar?
  def import
    walk_incoming do |filename, extname|
      @pool.post do
        begin
          process_incoming_file filename, extname
        rescue => exception
          STDERR.puts "#{filename}: #{exception.message}"
        end
      end
    end

    @pool.shutdown
    @pool.wait_for_termination

    remove_empty_dirs @incoming_path
  end

  # TODO: What is the most efficient way to scan folders?
  def walk_incoming
    Dir.glob(@incoming_path + '**' + '*') do |filename|
      next unless File.file? filename
      extname = File.extname filename
      next if extname.empty?
      yield filename, extname.downcase
    end
  end

  def process_incoming_file filename, extname
    unless MEDIA_EXTENSIONS.include? extname
      puts "#{filename} not in media extensions list" if VERBOSE
      return
    end

    digest = get_file_digest filename

    if @digest_database.exist? digest
      remove_duplicate_file filename
      return
    end

    if TEST_CORRUPTED && ALL_IMAGE_EXTENSIONS.include?(extname) && is_image_corrupted?(filename)
      STDERR.puts "#{filename} is corrupted"
      return
    end

    media_info = get_media_info filename, extname

    @storage_mutex.synchronize do
      if @digest_database.exist? digest
        remove_duplicate_file filename
        return
      end

      output_path, output_filename = get_output_filename media_info, filename, extname

      ensure_dir_exists(@storage_path + output_path)
      
      FileUtils.mv filename, (@storage_path + output_path + output_filename)

      puts "#{output_path + output_filename}" if VERBOSE
        
      @digest_database.insert (output_path + output_filename), digest
    end
  end

  def get_output_filename media_info, filename, extname
    output_path = Pathname.new(media_info[:type]) + (media_info[:camera] || "unknown camera") + media_info[:datetime].strftime("%Y") + media_info[:datetime].strftime("%m")
    output_basename = media_info[:datetime].strftime("%Y-%m-%d %H-%M-%S")

    check_if_exist = Proc.new do |output_filename|
      output_filename += extname
      unless (@storage_path + output_path + output_filename).exist?
        return output_path, output_filename
      end
    end

    check_if_exist.call(output_basename)

    source_basename = File.basename(filename, extname)

    check_if_exist.call("#{output_basename} #{source_basename}")

    for i in 1..10_000
      check_if_exist.call("#{output_basename} #{source_basename} #{i}")
    end

    STDERR.puts (error_string = "Unable to store #{filename}, 10k filename limit exeeded")
    raise error_string
  end

  CORRUPT_REGEXP = Regexp.new('corrupt', Regexp::IGNORECASE)

  def is_image_corrupted? filename
    identify_result, status = Open3.capture2e('magick', 'identify', '-verbose', filename)

    unless status.success?
      return true
    end

    if CORRUPT_REGEXP.match? identify_result
      return true
    end

    return false
  end

  def get_jpeg_media_info filename
    media_info = {type: "photos"}

    begin
      exif = EXIFR::JPEG.new(filename)

      media_info[:datetime] = if exif.date_time_original
        exif.date_time_original
      elsif exif.date_time
        exif.date_time
      end

      if exif.model
        media_info[:camera] = exif.model
      end

    rescue
    end

    return media_info
  end

  QUICKTIME_MODEL_REGEXP = /^\s*com\.apple\.quicktime\.model: (.*)/
  QUICKTIME_CREATIONDATE_REGEXP = /^\s*com\.apple\.quicktime\.creationdate: (.*)/

  def get_video_media_info filename
    media_info = {type: "videos"}

    result, status = Open3.capture2e('ffprobe', filename)

    if status.success?
      if match = result.match(QUICKTIME_MODEL_REGEXP)
        media_info[:camera] = match[1]
      end

      if match = result.match(QUICKTIME_CREATIONDATE_REGEXP)
        media_info[:datetime] = DateTime.parse(match[1])
      end
    end

    return media_info
  end

  PNG_DATE_MODIFY_REGEXP = /^\s*date:modify: (.*)/

  def get_png_media_info filename
    media_info = {type: "screenshots"}

    result, status = Open3.capture2e('magick', 'identify', '-verbose', filename)

    if status.success?
      if match = result.match(PNG_DATE_MODIFY_REGEXP)
        media_info[:datetime] = DateTime.rfc3339(match[1])
      end
    end

    return media_info
  end

  def get_media_info filename, extname
    media_info = if JPEG_EXTENSIONS.include? extname
      get_jpeg_media_info filename
    elsif PNG_EXTENSIONS.include? extname
      get_png_media_info filename
    elsif VIDEO_EXTENSIONS.include? extname
      get_video_media_info filename
    else
      {}
    end

    unless media_info[:datetime]
      media_info[:datetime] = File.mtime(filename)
    end

    return media_info
  end

  def get_file_digest filename
    File.open(filename, 'rb') do |io|
      digest = Digest::SHA512.new
      buffer = ""
      while io.read(40960, buffer)
        digest.update(buffer)
      end
      return digest.hexdigest
    end
  end

  def remove_duplicate_file filename
    puts "#{filename} is a duplicate" if VERBOSE
    FileUtils.rm filename
  end
end

incoming_path = Pathname.new(ENV['HOME']) + 'photo-inbox'
storage_path = Pathname.new(ENV['HOME']) + 'photo-storage'

VERBOSE = true
TEST_CORRUPTED = true
CONCURRENCY = Concurrent.physical_processor_count

measure_time do
  importer = Importer.new incoming_path, storage_path
  importer.import
end

# TODO: digest cache cleanup?