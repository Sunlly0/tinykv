clearFunc() {
	DES_FOLDER=/tmp
	for FOLDER in $(ls $DES_FOLDER); do
		#  截取test
		test=$(expr substr $FOLDER 1 4)
		if [ "$test" = "test" ]; then
			$(rm -fr $DES_FOLDER/$FOLDER)
		fi
	done
}
for ((i = 1; i <= 150; i++)); do
echo "test:$i"
	# check_results=$(make project2b)
	# check_results=$( go test -v -run TestBasicConfChange3B ./kv/test_raftstore )
	check_results=$( go test -v -run TestConfChangeRecoverManyClients3B ./kv/test_raftstore )
	# check_results=$( go test -v ./scheduler/server -check.f  TestRegionNotUpdate3C )     
	$(go clean -testcache)
	clearFunc
	if [[ $check_results =~ "FAIL" ]]; then
		echo "$check_results" > result3b.txt
		clearFunc
		break
	fi
done 