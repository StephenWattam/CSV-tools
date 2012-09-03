#!/bin/bash


./csvmerge.rb test/sample_orig.csv test/sample_addit.csv out.csv InnerMerge MemoryCache url -- url


./csvsplit.rb out.csv one.csv url count -- two.csv count2
