import datetime
import pandas as pd

dates = [datetime.date(2015, 4, 6), datetime.date(2015, 4, 7)]
hours = [1, 2]
meals = ['soup', 'salad']
columns = ['servings', 'revenue']

index = pd.MultiIndex.from_product((dates, hours, meals),
                                   names=['date', 'hour', 'meal'])
df = pd.DataFrame(index=index, columns=columns)
print(df)

# with loc()
print(df.loc[datetime.date(2015, 4, 6)].loc[1].loc['soup']['servings'])
print(df.loc[datetime.date(2015, 4, 6)].loc[1, 'soup']['servings'])
print(df.loc[datetime.date(2015, 4, 6), 1].loc['soup']['servings'])
# with ix()
print(df.loc[datetime.date(2015, 4, 6), 1].ix['soup', 'servings'])
print(df.loc[datetime.date(2015, 4, 6)].ix[(1, 'soup'), 'servings'])

print('If you uncomment any one of these two, it will fail:')
# with loc()
# print(df.loc[datetime.date(2015, 4, 6), 1, 'soup']['servings'])
# with ix()
# print(df.ix[(datetime.date(2015, 4, 6), 1, 'soup'), 'servings'])

