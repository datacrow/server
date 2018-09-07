#!/bin/sh
release=$(curl --silent "https://api.github.com/repos/datacrow/core/releases/latest" | grep -Po '"tag_name": "\K.*?(?=")')
wget https://github.com/datacrow/core/releases/download/${release}/datacrow-core.jar -O ./lib/datacrow-core.jar
