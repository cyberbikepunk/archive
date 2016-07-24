import datetime
import pandas as pd
import time
import timeit

dates = []
date = datetime.date(2015, 4, 6)
end_date = datetime.date(2015, 10, 7)
one_day = datetime.timedelta(days=1)
while date <= end_date:
    dates.append(date)
    date += one_day

hours = [_ for _ in range(24)]
meals = list('ABCD')
columns = list('abcdefgh')

index = pd.MultiIndex.from_product((dates, hours, meals),
                                   names=['date', 'hour', 'meal'])

df = pd.DataFrame(index=index, columns=columns)

print(df.head(11))
df.loc[datetime.date(2015, 8, 6)].loc[12].loc['B']['d'] = 0

df.loc[datetime.date(2015, 4, 6)].loc[12, 'B']['d'] = 0

df.loc[datetime.date(2015, 4, 6), 12].loc['B']['d'] = 0

print('This will fail:')
#df.loc[datetime.date(2015, 4, 6), 12, 'B']['d'])
