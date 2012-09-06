#!/bin/bash


./csvmerge test/sample_orig.csv test/sample_addit.csv out.csv InnerMerge MemoryCache url -- url


./csvsplit out.csv one.csv url count -- two.csv count2


./csvcat test/sample_orig.csv test/sample_huge.csv test/sample_addit.csv out.csv



./csvfilter test/citations.csv out.csv 100 "" FieldNotNull citations -- AllFieldsNotNull
