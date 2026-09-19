# BESS (Batarya Enerji Depolama Sistemi) Fiyat Arbitraj ve Optimizasyon Planı

Bu plan, geliştirilen Makine Öğrenmesi (Random Forest) elektrik fiyat tahmin modelinin çıktılarını, fiziksel bir Batarya Enerji Depolama Sistemi'nin (BESS) piyasa arbitraj optimizasyonuna bağlayarak projeyi analitik ve ticari bir seviyeye taşımayı hedefler.

---

## 📌 Proje Aşamaları ve Durum Takibi

- [x] **Adım 1: Veri Altyapısı ve Tahmin Pipeline Entegrasyonu (Data & Forecasting Pipeline)**
  - [x] SMARD API üzerinden saatlik elektrik fiyatları (`Price_EUR_MWh`) ve fiziksel şebeke verileri (`Load_MWh`, `Residual_Load_MWh`, `Solar_MWh`, `Wind_Onshore_MWh`, `Wind_Offshore_MWh`) çekildi (`market_data.py`).
  - [x] Çok haftalık zaman serisi birleştirme fonksiyonu (`fetch_smard_fundamentals`) oluşturuldu.
  - [x] Random Forest regressor modeli fiziksel piyasa temelleriyle eğitildi ve test kümesinde $R^2 \approx 0.84$ doğruluk seviyesine ulaşıldı (`rf_model.py`).
  - [x] Gerçekleşen vs. Tahmin edilen fiyat karşılaştırma grafiği (`forecast_comparison.png`) üretildi.

- [x] **Adım 2: BESS Teknik ve Finansal Parametre Modülünün Geliştirilmesi (`bess_optimizer.py` -> `BESSConfig`)**
  - [x] Batarya nominal kapasitesi (2.0 MWh) ve güç inverter limiti (1.0 MW) tanımlandı.
  - [x] Gidiş-Dönüş Verimliliği (Round-Trip Efficiency - RTE: %86.5, $\eta_c = 0.93, \eta_d = 0.93$) modellendi.
  - [x] Şarj Durumu sınırları (State of Charge - SoC: $SoC_{min} = 10\%$, $SoC_{max} = 90\%$) ile pil ömrü koruması sağlandı.
  - [x] Batarya hücre döngü yıpranma maliyeti (Degradation Cost = 5.00 EUR/MWh throughput) eklendi.

- [x] **Adım 3: Lineer Programlama ile Arbitraj Optimizasyon Motoru (`bess_optimizer.py` -> `optimize_bess_dispatch`)**
  - [x] `scipy.optimize.linprog` (HiGHS metodu) kullanılarak saatlik şarj ($P_{ch,t}$) ve deşarj ($P_{dis,t}$) optimizasyonu formüle edildi.
  - [x] Amaç Fonksiyonu: Net arbitraj kârını maksimize etme (fiyat geliri - şarj maliyeti - döngü yıpranması).
  - [x] Kısıtlar: Güç kısıtları, SoC dinamik süreklilik eşitliği, SoC alt/üst limitleri ve döngü kapanış eşitliği ($SoC_{end} = SoC_{start} = 50\%$).

- [x] **Adım 4: Tahmin Odaklı vs. Kusursuz Öngörü Backtesti (`run_bess_comparison`)**
  - [x] **Kusursuz Öngörü (Perfect Foresight - Benchmark):** Fiyatların önceden %100 bilindiği teorik tavan kâr (2,087.86 €).
  - [x] **Tahmin Odaklı İşletim (Forecast-Driven Strategy):** Random Forest tahmin sinyalleriyle planlanan ve gerçek piyasa fiyatlarıyla uzlaştırılan strateji (2,064.88 €).
  - [x] Değer Yakalama Oranı (Forecast Value Capture Ratio): **%98.9** olarak hesaplandı.

- [x] **Adım 5: Finansal Performans ve Operasyonel Raporlama (Financial & Operational KPIs)**
  - [x] Net Arbitraj Kârı: **2,064.88 €** (5 günlük test süresince).
  - [x] Ortalama Deşarj Fiyatı: **281.39 €/MWh**, Ortalama Şarj Fiyatı: **92.85 €/MWh**.
  - [x] Gerçekleşen Fiyat Farkı (Realized Spread): **188.54 €/MWh**.
  - [x] Eşdeğer Tam Döngü Sayısı (Full Cycle Equivalent): **8.52 döngü** (günlük ~1.7 döngü, lityum-iyon bataryalar için ideal işletme rejimi).

- [x] **Adım 6: BESS Operasyon Paneli Görselleştirmesi (`bess_dispatch_results.png`)**
  - [x] 3 panelli profesyonel gösterge tablosu üretildi:
    1. Elektrik Fiyat Sinyalleri (Gerçekleşen vs. Tahmin)
    2. Optimal BESS Şarj/Deşarj Güç Dağılımı (MW)
    3. Batarya Şarj Durumu (SoC %) ve Kümülatif Net Kâr Eğrisi (€).

- [x] **Adım 7: Doğrulama, Test ve GitHub'a Push**
  - [x] Kodlar terminalde uçtan uca test edildi ve başarıyla tamamlandı.
  - [x] Değişiklikler commit'lenip GitHub deposuna gönderildi.

---

## 📊 Optimizasyon ve Backtest Sonuç Özeti

| Performans Metriği | Tahmin Odaklı (Forecast-Driven) | Kusursuz Öngörü (Perfect Foresight) |
|---|:---:|:---:|
| **Net Arbitraj Kârı** | **2,064.88 €** | **2,087.86 €** |
| **Brüt Kâr (Yıpranma Öncesi)** | 2,201.24 € | 2,235.00 € |
| **Toplam Satış Geliri** | 3,559.06 € | 3,778.77 € |
| **Toplam Şarj Maliyeti** | 1,357.82 € | 1,543.77 € |
| **Hücre Yıpranma Maliyeti** | 136.36 € | 147.14 € |
| **Ortalama Deşarj Fiyatı** | 281.39 €/MWh | 276.87 €/MWh |
| **Ortalama Şarj Fiyatı** | 92.85 €/MWh | 97.83 €/MWh |
| **Yakalanan Spread (Fark)** | **188.54 €/MWh** | **179.04 €/MWh** |
| **Tam Döngü (FCE)** | 8.52 döngü | 9.20 döngü |
| **Değer Yakalama Oranı** | **%98.9** | %100.0 |
