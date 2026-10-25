#!/bin/sh
# The installer for macOS and Linux:
#
#   curl -fsSL https://raw.githubusercontent.com/williammgb/universal-data-pipeline/main/deploy/install.sh | sh
#
# It puts compose.release.yaml and the udp launcher in UDP_HOME (default ~/udp), makes sources/
# and pipelines/ there, and adds that folder to PATH in ~/.bashrc and ~/.zshrc, so `udp up` works
# in any new terminal. When neither exists it writes ~/.profile, and ~/.zshrc when the shell is
# zsh or ~/.zprofile when it is not: a Mac terminal is zsh, which never reads ~/.profile. The
# folder goes into the line in single quotes, so a name with `"`, `$` or a backtick is taken as
# typed; one with a `:` or a line break, which PATH cannot hold, is refused before anything is
# installed. Running it again updates the compose file and the launcher; .env, sources/,
# pipelines/ and backups/ are never touched.
#
# Every file is fetched to a temporary name first and only moved into place once all of them
# arrived, so a failed download leaves the files from before as they were.
#
# UDP_INSTALL_FROM=<folder> copies the files from that folder instead of downloading them, and
# UDP_SKIP_PATH=1 leaves PATH and the shell files alone: both are for the tests.
set -eu

base=https://raw.githubusercontent.com/williammgb/universal-data-pipeline/main/deploy
files="compose.release.yaml udp"
from=${UDP_INSTALL_FROM:-}

mkdir -p "${UDP_HOME:-$HOME/udp}"
home=$(cd "${UDP_HOME:-$HOME/udp}" && pwd)

if [ "${UDP_SKIP_PATH:-}" != 1 ]; then
  case "$home" in
    *:* | *"
"*)
      echo "udp: $home cannot go on PATH: a folder with a : or a line break in its name cannot; set UDP_HOME to another folder and run this again" >&2
      exit 1
      ;;
  esac
fi

if [ -z "$from" ] && ! command -v curl >/dev/null 2>&1; then
  echo "udp: the installer needs curl: install it, then run this again" >&2
  exit 1
fi

cleanup() {
  for name in $files; do
    rm -f "$home/.$name.download"
  done
}
trap cleanup EXIT
trap 'exit 130' INT TERM

for name in $files; do
  if [ -n "$from" ]; then
    cp "$from/$name" "$home/.$name.download"
  else
    curl -fsSL "$base/$name" -o "$home/.$name.download"
  fi
done
for name in $files; do
  mv -f "$home/.$name.download" "$home/$name"
done
chmod +x "$home/udp"
mkdir -p "$home/sources" "$home/pipelines"

# The folder in single quotes, each ' in it written as '\'' : nothing in it is expanded or run.
quoted="'$(printf '%s' "$home" | sed "s/'/'\\\\''/g")'"
line="export PATH=$quoted:\"\$PATH\"  # udp"
# The line as installers before this one wrote it.
older="export PATH=\"$home:\$PATH\"  # udp"

# Appends the PATH line to the file named, unless it, or the older form, is already in it.
add_line() {
  if [ -f "$1" ] && { grep -qxF "$line" "$1" || grep -qxF "$older" "$1"; }; then
    return 0
  fi
  if [ -s "$1" ] && [ -n "$(tail -c 1 "$1")" ]; then
    echo >> "$1"
  fi
  printf '%s\n' "$line" >> "$1"
}

if [ "${UDP_SKIP_PATH:-}" = 1 ]; then
  next="Add $home to PATH, then run: udp up"
else
  case ":$PATH:" in
    *":$home:"*)
      next="Run: udp up"
      ;;
    *)
      found=
      for rc in "$HOME/.bashrc" "$HOME/.zshrc"; do
        if [ -f "$rc" ]; then
          add_line "$rc"
          found=1
        fi
      done
      if [ -z "$found" ]; then
        add_line "$HOME/.profile"
        case "${SHELL:-}" in
          */zsh) add_line "$HOME/.zshrc" ;;
          *) add_line "$HOME/.zprofile" ;;
        esac
      fi
      next="Open a new terminal, then run: udp up"
      ;;
  esac
fi

echo "Installed udp in $home"
echo "$next"
