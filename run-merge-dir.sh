#!/bin/bash

. environment.sh

results=`ls $@`
merged_dir=$@/merged

rm .sizes

for result in $results
do
	size=`tdbquery --results CSV --loc="$@"/"$result" 'select (count(?g) as ?mc) where { graph ?g {} }' | tail -n 1`
	echo -e -n "[info] ${result}\\t${size}"
	echo -e "${result},${size}" >> .sizes
done

echo ""

ordered_set=(`cat .sizes | sort -n -r -t , -k 2 | cut -d , -f 1`)
size=${#ordered_set[@]}

foundation=${ordered_set[0]}

echo "[info] using ${foundation} as foundation - cloning as 'merged'"
cp -R $@/$foundation $merged_dir

for ((i = 1; i < size; i++))
do
	echo ""
	echo ""
	echo ""
	echo "[info] merging $@/${ordered_set[$i]} into ${merged_dir}"
	echo ""
	echo ""
	echo ""

	./merge.sh "$@"/"${ordered_set[$i]}" "$merged_dir"
done

rm .sizes
