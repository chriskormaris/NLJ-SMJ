## Note: The first 4 equi-joins run NLJ.
## The implementation for Nested Loop Join (NLJ) is the same for all ".jar" files.
## The implementation for Sort Merge Join (SMJ) differs for each ".jar" file.

## joinalgs.jar
echo 'Running equi-joins using "joinalgs.jar"'
printf '\n'
# equi-join 1
echo 'Running equi-join 1...'
java -jar joinalgs.jar -f1 testdata/D.csv -a1 3 -f2 testdata/C.csv -a2 0 -j NLJ -m 200 -o TESTING > runs/runEqui-Join1.txt
# equi-join 2
echo 'Running equi-join 2...'
java -jar joinalgs.jar -f1 testdata/D.csv -a1 3 -f2 testdata/B.csv -a2 0 -j NLJ -m 200 -o TESTING > runs/runEqui-Join2.txt
# equi-join 3
echo 'Running equi-join 3...'
java -jar joinalgs.jar -f1 testdata/A.csv -a1 3 -f2 testdata/E.csv -a2 0 -j NLJ -m 200 -o TESTING > runs/runEqui-Join3.txt
# equi-join 4
echo 'Running equi-join 4...'
java -jar joinalgs.jar -f1 testdata/B.csv -a1 1 -f2 testdata/B.csv -a2 2 -j NLJ -m 200 -o TESTING > runs/runEqui-Join4.txt
# equi-join 5
echo 'Running equi-join 5...'
java -jar joinalgs.jar -f1 testdata/D.csv -a1 3 -f2 testdata/C.csv -a2 0 -j SMJ -m 200 -t tmp -o TESTING > runs/runEqui-Join5.txt
# equi-join 6
echo 'Running equi-join 6...'
java -jar joinalgs.jar -f1 testdata/D.csv -a1 3 -f2 testdata/B.csv -a2 0 -j SMJ -m 200 -t tmp -o TESTING > runs/runEqui-Join6.txt
# equi-join 7
echo 'Running equi-join 7...'
java -jar joinalgs.jar -f1 testdata/A.csv -a1 3 -f2 testdata/E.csv -a2 0 -j SMJ -m 200 -t tmp -o TESTING > runs/runEqui-Join7.txt
# equi-join 8
echo 'Running equi-join 8...'
java -jar joinalgs.jar -f1 testdata/B.csv -a1 1 -f2 testdata/B.csv -a2 2 -j SMJ -m 200 -t tmp -o TESTING > runs/runEqui-Join8.txt
printf '\n'

## joinalgsUsingThreads.jar
echo 'Running equi-joins using "joinalgsUsingThreads.jar"'
printf '\n'
# equi-join 5
echo 'Running equi-join 5...'
java -jar joinalgsUsingThreads.jar -f1 testdata/D.csv -a1 3 -f2 testdata/C.csv -a2 0 -j SMJ -m 200 -t tmp -o TESTING > runsUsingThreads/runEqui-Join5.txt
# equi-join 6
echo 'Running equi-join 6...'
java -jar joinalgsUsingThreads.jar -f1 testdata/D.csv -a1 3 -f2 testdata/B.csv -a2 0 -j SMJ -m 200 -t tmp -o TESTING > runsUsingThreads/runEqui-Join6.txt
# equi-join 7
echo 'Running equi-join 7...'
java -jar joinalgsUsingThreads.jar -f1 testdata/A.csv -a1 3 -f2 testdata/E.csv -a2 0 -j SMJ -m 200 -t tmp -o TESTING > runsUsingThreads/runEqui-Join7.txt
# equi-join 8
echo 'Running equi-join 8...'
java -jar joinalgsUsingThreads.jar -f1 testdata/B.csv -a1 1 -f2 testdata/B.csv -a2 2 -j SMJ -m 200 -t tmp -o TESTING > runsUsingThreads/runEqui-Join8.txt
printf '\n'

## joinalgsAlternativeMerge.jar
echo 'Running equi-joins using "joinalgsAlternativeMerge.jar"'
printf '\n'
# equi-join 5
echo 'Running equi-join 5...'
java -jar joinalgsAlternativeMerge.jar -f1 testdata/D.csv -a1 3 -f2 testdata/C.csv -a2 0 -j SMJ -m 200 -t tmp -o TESTING > runsAlternativeMerge/runEqui-Join5.txt
# equi-join 6
echo 'Running equi-join 6...'
java -jar joinalgsAlternativeMerge.jar -f1 testdata/D.csv -a1 3 -f2 testdata/B.csv -a2 0 -j SMJ -m 200 -t tmp -o TESTING > runsAlternativeMerge/runEqui-Join6.txt
# equi-join 7
echo 'Running equi-join 7...'
java -jar joinalgsAlternativeMerge.jar -f1 testdata/A.csv -a1 3 -f2 testdata/E.csv -a2 0 -j SMJ -m 200 -t tmp -o TESTING > runsAlternativeMerge/runEqui-Join7.txt
# equi-join 8
echo 'Running equi-join 8...'
java -jar joinalgsAlternativeMerge.jar -f1 testdata/B.csv -a1 1 -f2 testdata/B.csv -a2 2 -j SMJ -m 200 -t tmp -o TESTING > runsAlternativeMerge/runEqui-Join8.txt
printf '\n'

## joinalgsAlternativeMergeUsingThreads.jar
echo 'Running equi-joins using "joinalgsAlternativeMergeUsingThreads.jar"'
printf '\n'
# equi-join 5
echo 'Running equi-join 5...'
java -jar joinalgsAlternativeMergeUsingThreads.jar -f1 testdata/D.csv -a1 3 -f2 testdata/C.csv -a2 0 -j SMJ -m 200 -t tmp -o TESTING > runsAlternativeMergeUsingThreads/runEqui-Join5.txt
# equi-join 6
echo 'Running equi-join 6...'
java -jar joinalgsAlternativeMergeUsingThreads.jar -f1 testdata/D.csv -a1 3 -f2 testdata/B.csv -a2 0 -j SMJ -m 200 -t tmp -o TESTING > runsAlternativeMergeUsingThreads/runEqui-Join6.txt
# equi-join 7
echo 'Running equi-join 7...'
java -jar joinalgsAlternativeMergeUsingThreads.jar -f1 testdata/A.csv -a1 3 -f2 testdata/E.csv -a2 0 -j SMJ -m 200 -t tmp -o TESTING > runsAlternativeMergeUsingThreads/runEqui-Join7.txt
# equi-join 8
echo 'Running equi-join 8...'
java -jar joinalgsAlternativeMergeUsingThreads.jar -f1 testdata/B.csv -a1 1 -f2 testdata/B.csv -a2 2 -j SMJ -m 200 -t tmp -o TESTING > runsAlternativeMergeUsingThreads/runEqui-Join8.txt
printf '\n'

rm TESTING
echo 'Everything is done! Goodbye!'

