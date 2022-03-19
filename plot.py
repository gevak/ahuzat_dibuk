"""Used to plot parking lot data saved on Google Cloud Storage.
Can be used to plot the details for a single parking lot, or all of the parking lots in one large graph.
"""

from bidi import algorithm as bidialg
import seaborn as sns
from google.cloud import storage
import pandas as pd
import pyarrow as pa
import matplotlib.pyplot as plt

DAY_START = 5
BORING_LOTS = ['חניון רפואת שיניים', 'חניון הנדסה']
DAYS_HEBREW = ['ראשון', 'שני', 'שלישי', 'רביעי', 'חמישי', 'שישי', 'שבת']
DAYS_ENGLISH = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']

def loadData(bucket_name='ahuzat-data-bucket', path='data.feather'):
  client = storage.Client()
  bucket = client.get_bucket(bucket_name)
  blob = bucket.get_blob(path)
  if blob:
    reader = pa.BufferReader(blob.download_as_bytes())
    return pd.read_feather(reader)
  return pd.DataFrame()

def selectLot(df):
  lot_names = df['lot'].drop_duplicates()
  for i, lot in enumerate(lot_names):
    print('[%d] %s' % (i, lot[::-1]))
  num = int(input("Select lot number: "))
  return lot_names[num]

def heatPlot(df, lot_name, day_start, show_labels=True, transpose_axis=False, rtl=False, hebrew=True, ax=None, title_size=28):
  if not ax:
    ax = plt.gca()
  fixTimes(df, day_start)
  lot_df = df[df.lot == lot_name][['hour', 'day', 'status']]
  df_m = lot_df.groupby(['hour', 'day']).mean().unstack(level=0)

  title = lot_name
  xlabels = ['%2d:00' % (x%24) for x in range(day_start, 24 + day_start)]
  xlabelsize=14
  ylabelsize=20
  if show_labels:
    if hebrew:
      ylabels = [s[::-1] for s in DAYS_HEBREW]
      xlabel = 'שעה'[::-1]
      ylabel = 'יום'[::-1]
    else:
      ylabels = DAYS_ENGLISH
      xlabel = 'Time'
      ylabel = 'Day'
  else:
    xlabels = []
    ylabels = []
    xlabel = None
    ylabel = None

  if transpose_axis:
    (xlabels, ylabels) = (ylabels, xlabels)
    (xlabel, ylabel) = (ylabel, xlabel)
    df_m = lot_df.groupby(['day', 'hour']).mean().unstack(level=0)
    xlabelsize, ylabelsize = ylabelsize, xlabelsize

  sns.heatmap(df_m, vmin=0, vmax=1, 
              cmap=sns.color_palette("YlOrBr", as_cmap=True), cbar=show_labels and not rtl,
              xticklabels=xlabels, yticklabels=ylabels, ax=ax, annot=show_labels, fmt='.2f')
  ax.set_title(bidialg.get_display(title), fontsize=title_size, fontweight='bold')
  ax.set_xlabel(xlabel, fontsize = 22, fontweight='bold')
  ax.set_ylabel(ylabel, fontsize = 22, fontweight='bold')

  if show_labels:
    title += ' - עומס'
    ax.set_xticklabels(xlabels, fontsize = xlabelsize)
    ax.set_yticklabels(ylabels, fontsize = ylabelsize)
    # Draw arrow below x axis
    ax.plot((1), (7), ls="", marker=">", ms=20, color="k",
        transform=ax.get_yaxis_transform(), clip_on=False)
    plt.axhline(y=7, linewidth=3, color="k", clip_on=False)

  if rtl:
    ax.invert_xaxis() 
    ax.yaxis.tick_right()
    ax.yaxis.set_label_position("right")

def getLotAverageHeat(df, lot_name):
  return df[df.lot == lot_name]['status'].mean()

def heatPlotAll(df, day_start):
  lot_names = df['lot'].drop_duplicates()
  lot_names = sorted(lot_names, key=lambda lot_name: getLotAverageHeat(df, lot_name))
  _, axs = plt.subplots(ncols=7, nrows=7)
  plt.subplots_adjust(hspace = 0.1, wspace = 0.1, bottom=.01, top=.99, left=.01, right=.99)
  for i, lot in enumerate(lot_names):
    ax = axs[i//7][i%7]
    ax.patch.set_edgecolor('black')  
    ax.patch.set_linewidth('3')  
    print("Plotting lot number", i)
    heatPlot(df, lot, day_start=day_start, show_labels=False, ax=ax, title_size=14)

def removeBoringLots(df):
  return df[~df.lot.isin(BORING_LOTS)].copy()

def fixTimes(df, day_start):
  # Fix so that each day starts at day_start:00 hours
  time = df['time'] - pd.Timedelta(hours=day_start)
  df['day'] = (time.dt.weekday + 1) % 7
  df['hour'] = time.dt.hour

def test():
  print('Loading data')
  df = loadData()
  import ipdb; ipdb.set_trace()

def mainSingle():
  print('Loading data')
  df = loadData()
  print('Loaded DF with %d rows' % df.shape[0])
  lot = selectLot(df)
  plt.rcParams['figure.figsize'] = [24, 10]
  heatPlot(df, lot, DAY_START, show_labels=True)
  plt.show()

def mainAll():
  print('Loading data')
  df = loadData()
  print('Loaded DF with %d rows' % df.shape[0])
  df = removeBoringLots(df)
  plt.rcParams['figure.figsize'] = [25, 25]
  heatPlotAll(df, DAY_START)
  plt.savefig("parking.png")

if __name__ == '__main__':
    mainAll()