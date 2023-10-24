Task : 抹茶交易所幣種行情錄製, 訂單錄製, 錢包錄製. 透過交易所得websocket api

Need2Do:
    Class objects / functions / codes with #MustDO tag on it

Note:

    1.有些地方牽涉到使用交易所得restful api去獲取初始資訊
      在實踐上我是使用ccxt套件的api去interact with restful api of exchange
      這在發案範圍外，並不一定要實作，在這些內容上都加了#NOT MUST DO的tag

    2.在template中的websocket subscription, 我是透過pybit的套件，我不確定
      抹茶交易所是否有人也有做出類似的套件，這個套件等於是包裝了在subscription過程中的一些
      code(例如一些endpoint的填寫等等)，若無則需自行implement
      
deadline:
    End of 2023
