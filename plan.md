# BESS (Batarya Enerji Depolama Sistemi) Fiyat Arbitraj ve Optimizasyon Planı

Bu plan, geliştirilen Makine Öğrenmesi (Random Forest) elektrik fiyat tahmin modelinin çıktılarını, fiziksel bir Batarya Enerji Depolama Sistemi'nin (BESS) piyasa arbitraj optimizasyonuna bağlayarak projeyi analitik ve ticari bir seviyeye taşımayı hedefler.

---

## 📌 Proje Aşamaları ve Durum Takibi

- [x] **Adım 1: Veri Altyapısı ve Tahmin Pipeline Entegrasyonu (Data & Forecasting Pipeline)**
  - [x] SMARD API üzerinden saatlik elektrik fiyatları (`Price_EUR_MWh`) ve fiziksel şebeke verileri (`Load_MWh`, `Residual_Load_MWh`, `Solar_MWh`, `Wind_Onshore_MWh`, `Wind_Offshore_MWh`) çekildi (`market_data.py`).
  - [x] Çok haftalık zaman serisi birleştirme fonksiyonu (`fetch_smard_fundamentals`) oluşturuldu.
  - [x] Random Forest regressor modeli fiziksel piyasa temelleriyle eğitildi ve test kümesinde $R^2 \approx 0.83$ doğruluk seviyesine ulaşıldı (`rf_model.py`).
  - [x] Gerçekleşen vs. Tahmin edilen fiyat karşılaştırma grafiği (`forecast_comparison.png`) üretildi.

- [ ] **Adım 2: BESS Teknik ve Finansal Parametre Modülünün Geliştirilmesi (`bess_model.py` / `bess_config.py`)**
  - [ ] Batarya nominal kapasitesi (örn. 2 MWh veya 4 MWh) ve güç limiti (örn. 1 MW) tanımlanması.
  - [ ] Gidiş-Dönüş Verimliliği (Round-Trip Efficiency - RTE, $\eta_c, \eta_d \approx 90-93\%$) modellemesi.
  - [ ] Şarj Durumu sınırları (State of Charge - SoC: $SoC_{min} = 10\%$, $SoC_{max} = 90\%$).
  - [ ] Batarya döngü yıpranma maliyeti (Battery Cell Degradation / Cycling Cost: EUR/MWh).

- [ ] **Adım 3: Lineer Programlama ile Arbitraj Optimizasyon Motoru (`bess_optimizer.py`)**
  - [ ] `scipy.optimize.linprog` kullanılarak saatlik şarj ($P_{ch,t}$) ve deşarj ($P_{dis,t}$) optimizasyonu.
  - [ ] Amaç Fonksiyonu (Objective): Net kârın maksimize edilmesi ($\max \sum [P_{dis,t} \cdot \lambda_t - P_{ch,t} \cdot \lambda_t - \text{Maliyet}]$).
  - [ ] Kısıtlar (Constraints): Güç sınırları, SoC dinamik süreklilik denklemi, SoC alt/üst limitleri ve döngü kapanış eşitliği ($SoC_{end} = SoC_{start}$).

- [ ] **Adım 4: Tahmin Odaklı vs. Kusursuz Öngörü Backtesti (Forecast-Driven vs. Perfect Foresight)**
  - [ ] **Kusursuz Öngörü (Perfect Foresight - Benchmark):** Fiyatların önceden %100 bilindiği teorik tavan getiri.
  - [ ] **Tahmin Odaklı İşletim (Forecast-Driven Strategy):** Random Forest tahmin sinyalleriyle planlanan, ancak gerçekleşen piyasa fiyatlarıyla uzlaştırılan gerçekçi strateji.
  - [ ] Değer Yakalama Oranı (Value Capture Ratio: $\frac{\text{Kâr}_{tahmin}}{\text{Kâr}_{teorik}}$) hesabı.

- [ ] **Adım 5: Finansal Performans ve Operasyonel Raporlama (Financial & Operational KPIs)**
  - [ ] Toplam Arbitraj Geliri, Toplam Şarj Maliyeti ve Net Kâr (EUR).
  - [ ] Eşdeğer Tam Döngü Sayısı (Full Cycle Equivalent - FCE) ve batarya kullanım oranı.
  - [ ] Ortalama Şarj Fiyatı vs. Ortalama Deşarj Fiyatı (Spread Yakalama).
  - [ ] Negatif fiyatlı saatlerden sağlanan avantaj (Negatif fiyatta hem şarj olup hem ödeme alma etkisi).

- [ ] **Adım 6: BESS Operasyon Paneli Görselleştirmesi (`bess_dispatch_plot.png`)**
  - [ ] 3 panelli görsel gösterge tablosu:
    1. Elektrik Fiyatları (Actual vs Predicted)
    2. Batarya Şarj/Deşarj Güç Dağılımı (Dispatch Profile - MW)
    3. Batarya Şarj Durumu (SoC Zaman Serisi - %) ve Kümülatif Kâr (EUR).

- [ ] **Adım 7: Doğrulama, Test ve GitHub'a Push**
  - [ ] Kodların uçtan uca çalıştırılması.
  - [ ] Değişikliklerin ve üretilen grafiklerin GitHub deposuna commit ve push edilmesi.
