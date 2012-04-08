markdown : *.lhs
		pandoc -f markdown+lhs Introduction.md -o 01.md
		pandoc -f markdown+lhs lock-based-implementation.md -o 02.md
		pandoc -f markdown+lhs algorithm.md -o 03.md
		pandoc -f markdown+lhs Transactions.lhs -o 04.md
		pandoc -f markdown+lhs Journal.lhs -o 05.md
		pandoc -f markdown+lhs TransactionFH.lhs -o 06.md
		pandoc -f markdown+lhs FileHandling.lhs -o 07.md
		pandoc -f markdown+lhs FileHeader.lhs -o 08.md
        
		rm -f report.md
		touch report.md
		cat 01.md >> report.md
		cat 02.md >> report.md
		cat 03.md >> report.md
		cat 04.md >> report.md
		cat 05.md >> report.md
		cat 06.md >> report.md
		cat 07.md >> report.md
		cat 08.md >> report.md

		pandoc -f markdown+lhs report.md -o report.pdf
clean :
		rm -f *.aux
		rm -f *.log
		rm -f *.ptb
		rm -f *.pdf

cleanmd : 
		rm -f *.md
