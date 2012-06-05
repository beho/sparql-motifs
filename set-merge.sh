#!/bin/bash

. environment.sh

result_set=`ls $@`
merged_set_dir=$@/merged

rm .sizes

for result in $result_set
do
	size=`tdbquery --results CSV --loc="$@"/"$result" 'select (count(?g) as ?mc) where { graph ?g {} }' | tail -n 1`
	echo -e "[info] $result"\\t"$size"
	echo -e "$result","$size" >> .sizes
done

echo ""

ordered_set=(`cat .sizes | sort -n -r -t , -k 2 | cut -d , -f 1`)
size=${#ordered_set[@]}

foundation=${ordered_set[0]}

echo "[info] using ${foundation} as a foundation"
cp -R $@/$foundation $merged_set_dir

for ((i = 1; i < size; i++))
do
	echo ""
	echo ""
	echo ""
	echo "[info] merging $@/${ordered_set[$i]} into ${merged_set_dir}"
	echo ""
	echo ""
	echo ""

	./merge.sh "$@"/"${ordered_set[$i]}" "$merged_set_dir"
	# scala scripts/tdb-merge.scala "$@"/"${ordered_set[$i]}" "$merged_set_dir"
done
