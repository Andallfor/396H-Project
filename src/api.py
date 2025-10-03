# example api usage
from processing.database import Database
import pandas as pd
import matplotlib.pyplot as plt

db = Database()
cmt = db.query('SELECT * FROM comments')
print(cmt.info(verbose = True))
print(cmt.head())
print(cmt['body'].head())
print(cmt['num_sentences'].describe())
cmt['num_sentences'].hist(bins = 100)

plt.show()