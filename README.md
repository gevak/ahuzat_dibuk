# Ahuzat Dibuk - Tracking occupancy in Tel-Aviv parking lots

### main.py 
This module was set-up to be executed on Google Cloud Platform. 
I run it every 15 minutes to collect more data into a file called data.feather, which I store on the cloud.
It relies on the status that Ahuzat Ha-hof website has for (almost) every parking lot, which is either 'Free', 'Full', or 'Few places'.

### plot.py 
I execute this module on my personal computer, to download the data I've collected from the cloud and plot it.

## What I learned from this
1. You can use Google Cloud Platform pretty much for free. For three months you can even use expensive features with free credit that Google gives you.
2. Google Cloud's interface for working with files (blobs) is terrible. It looks nothing like the Python interface for working with files, which made it more complicated than necessary to migrate my code from working locally to working in the cloud. This is 2022, why is this not a seamless transition?
3. In an early stage of the project I mistakenly deleted multiple weeks worth of data when I messed around with the code. Backups are important :)
4. I was a bit careless, and once the script was working on the cloud I just let it run. After some time I found out the machine had run out of memory (it had very little) and the script was crashing every time. The only problem is that I found out about this a full month after it stopped working, so I lost a lot of data I could have collected. Monitoring is also important! :)
