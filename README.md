Sa OpenWeatherAPI-ja primaju se podaci o razlicitim cesticama na odredjenoj lokaciji. U ovom konkretnom slucaju grada Smederevo. Te cestice se zasebno izdvajaju na one najvaznije a to su PM2.5 i PM10. 
Njih unutar stream-processor-a obradjujem u konvertujem u AQI (Air quality index) , i consumeru saljem samo onu od 2 cestice koja je glavna tj preovladava u datom trenutnku.
Za svrhu aplikacije podaci se dobavljaju na svakih 10sekundi, tj producer na svakih 10 sekundi uzima podatke iz OpenWeatherAPI i salje ih na raw-data topic.
Consumer iz proc-data topika uzima podatke i salje ih aplikaciji preko Server.js.
Na kraj use unutar aplikacije iscrtava grafik AQI i ispisuje koja je trenutna vrednost, kada je poslednji put azuirana i koja cestica prevoladava trenutno!
