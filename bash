#!/bin/bash

# Lokalizacja - domyślnie bieżący katalog lub pierwszy argument
TARGET_DIR="${1:-.}"

# Sprawdzenie, czy lokalizacja istnieje
if [ ! -d "$TARGET_DIR" ]; then
    echo "Błąd: katalog '$TARGET_DIR' nie istnieje."
    exit 1
fi

# Tworzenie 5 katalogów od dnia dzisiejszego
for i in 0 1 2 3 4; do
    DIR_NAME=$(date -d "+$i days" +"%d-%m-%Y")
    FULL_PATH="$TARGET_DIR/$DIR_NAME"
    
    if [ -d "$FULL_PATH" ]; then
        echo "Katalog $DIR_NAME już istnieje - pomijam."
    else
        mkdir -p "$FULL_PATH"
        echo "Utworzono katalog: $DIR_NAME"
    fi
done