#!/bin/bash

rm temp.txt
rm res.txt
python CoOccurrenceMatrix.py input/ratingData.txt > temp.txt
python RatingMatrix.py input/ratingData.txt >> temp.txt
python Sum.py temp.txt > res.txt