## A simple script to import photos to my photo storage

# That tool moves photos out of your directories to it's own directory structure. It will delete your duplicates. Yes, you will not find your files where you put them! So please be carefull and better do not use this tool. At least until the moment I'll finish the documentation which will explain how it works and what to expect.

## Windows install

1. Install dependencies

```sh
choco install imagemagick
choco install ffmpeg
```

2. Install rubyinstaller

3. Open ruby shell from the start menu

```sh
gem install bundler
bundle install
```

4. In case of any problems with sqlite, try to install it manually

```sh
gem install sqlite3
```
