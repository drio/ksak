#!/bin/bash
set -e

CURR=$(cat help.go | grep version | grep const | awk -F\" '{print $2}')
echo "Current release is: $CURR"
echo -ne "New release: "
read NEW
echo "Updating from $CURR to $NEW"
echo -ne "Do you still want to do this? (<ctrl-c> to abort)"
read FOO
gsed -i "s|${CURR}|${NEW}|g" help.go
git add help.go
git commit -m "Bump version from $CURR to $NEW"
git tag v$NEW
git push --follow-tags -u origin main
git push -u origin v$NEW
