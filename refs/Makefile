all: disk_based_transactions.md
		pandoc --data-dir=datadir --self-contained -t slidy -s disk_based_transactions.md -o disk_based_transactions-slidy.html
		pandoc --data-dir=datadir --self-contained -t slidy -s poster.md -o poster.html
		pandoc -t s5 -s disk_based_transactions.md -o disk_based_transactions-s5.html
		pandoc -t dzslides -s disk_based_transactions.md -o disk_based_transactions-dzslides.html
#		pandoc -t beamer -s disk_based_transactions.md -o disk_based_transactions.pdf


clean:
		rm -rf *.html *.pdf
