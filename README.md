# major-city-air-quality-analysis
现在可以根据location id，默认获取2020年1月1日到2025年1月1日的数据，save成csv

前提是这个location包含2020年1月1日到2025年1月1日的数据

1. 先在project root directory创建.env，在里面放OpenAq的API key:
   
OPENAQ_API_KEY='your_API_key'

2.  然后在command line跑：pip install -r requirements.txt

3. 再在command line跑：python fetch_data.py

跳出“Enter location ID:”提示时

输入location id

？什么是location id？
在https://explore.openaq.org

点击一个有白色边框的紫色点点，找到此时url中的location。

比如：https://explore.openaq.org/?location=739&provider=119&sensors=false#12/38.92184/-77.01317

<<<<<<< HEAD
这个位置的location id就是739
=======
看url，这个位置的location id就是739
>>>>>>> 99d829ce2a15a8247d91af81bc8a4f0380d263c0
