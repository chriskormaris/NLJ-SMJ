:: run equi-join 1
java -cp .\bin project.Main -f1 testdata\D.csv -a1 3 -f2 testdata\C.csv -a2 0 -j NLJ -m 200 -o results\results1.csv
pause;
:: run equi-join 2
java -cp .\bin project.Main -f1 testdata\D.csv -a1 3 -f2 testdata\B.csv -a2 0 -j NLJ -m 200 -o results\results2.csv
pause;
:: run equi-join 3
java -cp .\bin project.Main -f1 testdata\A.csv -a1 3 -f2 testdata\E.csv -a2 0 -j NLJ -m 200 -o results\results3.csv
pause;
:: run equi-join 4
java -cp .\bin project.Main -f1 testdata\B.csv -a1 1 -f2 testdata\B.csv -a2 2 -j NLJ -m 200 -o results\results4.csv
pause;
:: run equi-join 5
java -cp .\bin project.Main -f1 testdata\D.csv -a1 3 -f2 testdata\C.csv -a2 0 -j SMJ -m 200 -t tmp -o results\results5.csv
pause;
:: run equi-join 6
java -cp .\bin project.Main -f1 testdata\D.csv -a1 3 -f2 testdata\B.csv -a2 0 -j SMJ -m 200 -t tmp -o results\results6.csv
pause;
:: run equi-join 7
java -cp .\bin project.Main -f1 testdata\A.csv -a1 3 -f2 testdata\E.csv -a2 0 -j SMJ -m 200 -t tmp -o results\results7.csv
pause;
:: run equi-join 8
java -cp .\bin project.Main -f1 testdata\B.csv -a1 1 -f2 testdata\B.csv -a2 2 -j SMJ -m 200 -t tmp -o results\results8.csv
pause;
