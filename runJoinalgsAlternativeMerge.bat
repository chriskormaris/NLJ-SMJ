:: THIS IS THE SLOWEST EQUI-JOIN SMJ ALGORITHM!

:: run equi-join 5
java -jar joinalgsAlternativeMerge.jar -f1 testdata\D.csv -a1 3 -f2 testdata\C.csv -a2 0 -j SMJ -m 200 -t tmp -o results\results5.csv
pause;
:: run equi-join 6
java -jar joinalgsAlternativeMerge.jar -f1 testdata\D.csv -a1 3 -f2 testdata\B.csv -a2 0 -j SMJ -m 200 -t tmp -o results\results6.csv
pause;
:: run equi-join 7
java -jar joinalgsAlternativeMerge.jar -f1 testdata\A.csv -a1 3 -f2 testdata\E.csv -a2 0 -j SMJ -m 200 -t tmp -o results\results7.csv
pause;
:: run equi-join 8
java -jar joinalgsAlternativeMerge.jar -f1 testdata\B.csv -a1 1 -f2 testdata\B.csv -a2 2 -j SMJ -m 200 -t tmp -o results\results8.csv
pause;
