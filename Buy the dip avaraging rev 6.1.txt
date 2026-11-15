# =============================================================
# CELL 0 - SETUP COLAB (aktif, tinggal Run All)
# =============================================================
!pip install -q yfinance pandas numpy openpyxl
from google.colab import drive
drive.mount('/content/drive')

# =============================================================
# SAHAM MURAH TAPI BELUM RUSAK
# Screening IDX: Buy the Dip / Averaging
# rev6-final - Perbaikan bug fast_info, growth_source, double-call _safe_float,
#              guard DataFrame kosong, dan hardening pipeline
# =============================================================

import os
import warnings
import time
import math
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")

# =============================================================
# CONFIG
# =============================================================
CONFIG = {
    "TICKER_FILE": "/content/drive/MyDrive/ticker/IDX2_561.txt",
    "OUTPUT_DIR": "/content/drive/MyDrive/",
    "OUTPUT_PREFIX": "Saham murah tapi belum rusak",
    "CACHE_DIR": "/content/drive/MyDrive/cache_fundamental/",

    "PERIOD": "2y",
    "INTERVAL": "1d",
    "BATCH_SIZE": 50,
    "RETRY": 2,
    "MIN_BARS": 250,

    "RSI_PERIOD": 14,
    "STOCH_K": 14,
    "STOCH_SLOW": 3,
    "STOCH_D": 3,
    "MA_PERIODS": [50, 100, 200],
    "HIGH52_BARS": 252,
    "VALUE_AVG_DAYS": 20,

    "FUND_WORKERS": 8,
    "FUND_RETRY": 3,
    "FUND_CACHE_MAX_AGE_DAYS": 7,

    "M1_ROE_MIN": 12.0,
    "M1_ROE_IDEAL": 15.0,
    "M1_DER_MAX": 1.0,
    "M1_DER_IDEAL": 0.8,
    "M1_GROWTH_MIN": 0.0,
    "M1_DIV_MIN": 5.0,
    "M1_MKTCAP_MIN_T": 2.0,
    "M1_MKTCAP_IDEAL_T": 5.0,
    "M1_MAX_OUT": 25,
    "M1_FILTER_LIQUIDITY": True,
    "M1_VALUE_MIN_M": 10.0,

    "M2_RSI_MAX": 25.0,
    "M2_MA200_TOL": 1.05,
    "M2_DD_MIN": 15.0,
    "M2_DD_MAX": 35.0,
    "M2_STOCH_MAX": 20.0,
    "M2_MIN_PASS": 3,
    "M2_MAX_OUT": 25,

    "M3_PIVOT_WINDOW": 5,
    "M3_CLUSTER_TOL": 0.02,
    "M3_MIN_TOUCH": 3,
    "M3_TOUCH_TOL": 0.015,
    "M3_DYNAMIC_TOL": 0.97,
    "M3_NEAR_MIN": 1.0,
    "M3_NEAR_MAX": 1.03,
    "M3_DEMAND_VOL_MULT": 1.5,
    "M3_DEMAND_RISE_PCT": 8.0,
    "M3_DEMAND_LOOKAHEAD": 5,
    "M3_FALLBACK_LOW_DAYS": 60,
    "M3_SUPPORT_MAJOR_MAX_DROP": 0.25,
    "M3_MAX_OUT": 25,

    "M4_E1_ALLOC": 0.40,
    "M4_E2_ALLOC": 0.30,
    "M4_E3_ALLOC": 0.30,
    "M4_E1_RSI_MAX": 30.0,
    "M4_E1_SUPPORT_TOL": 1.02,
    "M4_E2_DD_MIN": 0.05,
    "M4_E2_DD_MAX": 0.08,
    "M4_E3_DD_MIN": 0.08,
    "M4_E3_DD_MAX": 0.12,
    "M4_E3_ROE_MIN": 15.0,
    "M4_E3_DER_MAX": 1.0,
    "M4_MAX_OUT": 25,

    "M5_ROE_MIN": 15.0,
    "M5_DER_MAX": 1.0,
    "M5_DIV_MIN": 5.0,
    "M5_RSI_MAX": 30.0,
    "M5_DD_MIN": 15.0,
    "M5_DD_MAX": 35.0,
    "M5_VALUE_MIN_M": 10.0,
}


# =============================================================
# EMBEDDED TICKER LIST (IDX2 561) - fallback jika file tidak ada
# =============================================================
EMBEDDED_TICKERS = [
    "AADI.JK", "ACRO.JK", "ACST.JK", "ADCP.JK", "ADES.JK", "ADMF.JK", "ADMR.JK",
    "AEGS.JK", "AGAR.JK", "AGII.JK", "AGRS.JK", "AHAP.JK", "AISA.JK", "AKKU.JK",
    "AKPI.JK", "AKSI.JK", "ALDO.JK", "ALII.JK", "ALKA.JK", "ALMI.JK", "ALTO.JK",
    "AMAG.JK", "AMAN.JK", "AMAR.JK", "AMIN.JK", "AMMN.JK", "AMMS.JK", "AMOR.JK",
    "APIC.JK", "APII.JK", "APLI.JK", "ARCI.JK", "AREA.JK", "ARGO.JK", "ARKA.JK",
    "ARKO.JK", "ARTA.JK", "ASGR.JK", "ASHA.JK", "ASJT.JK", "ASLC.JK", "ASLI.JK",
    "ASMI.JK", "ASPI.JK", "ASPR.JK", "ATAP.JK", "ATIC.JK", "ATLA.JK", "AVIA.JK",
    "AWAN.JK", "AXIO.JK", "AYAM.JK", "AYLS.JK", "BABP.JK", "BABY.JK", "BACA.JK",
    "BAIK.JK", "BAJA.JK", "BANK.JK", "BAPI.JK", "BATR.JK", "BAUT.JK", "BBLD.JK",
    "BBSI.JK", "BBSS.JK", "BCIC.JK", "BDKR.JK", "BEBS.JK", "BEEF.JK", "BEER.JK",
    "BELI.JK", "BELL.JK", "BESS.JK", "BHAT.JK", "BIKE.JK", "BIMA.JK", "BINA.JK",
    "BINO.JK", "BKDP.JK", "BLES.JK", "BLOG.JK", "BLTZ.JK", "BLUE.JK", "BMBL.JK",
    "BMHS.JK", "BMSR.JK", "BMTR.JK", "BNBR.JK", "BOAT.JK", "BOBA.JK", "BOGA.JK",
    "BOLA.JK", "BPFI.JK", "BPII.JK", "BPTR.JK", "BRAM.JK", "BREN.JK", "BRRC.JK",
    "BSBK.JK", "BSML.JK", "BTEK.JK", "BTON.JK", "BUAH.JK", "BUDI.JK", "BUKK.JK",
    "BUVA.JK", "CAKK.JK", "CAMP.JK", "CANI.JK", "CARS.JK", "CASA.JK", "CBDK.JK",
    "CBPE.JK", "CBRE.JK", "CBUT.JK", "CCSI.JK", "CDIA.JK", "CFIN.JK", "CGAS.JK",
    "CHEK.JK", "CHEM.JK", "CHIP.JK", "CINT.JK", "CLAY.JK", "CLPI.JK", "CMNP.JK",
    "CMNT.JK", "CMPP.JK", "CNMA.JK", "COAL.JK", "COCO.JK", "COIN.JK", "CPRO.JK",
    "CRAB.JK", "CRSN.JK", "CSIS.JK", "CSMI.JK", "CSRA.JK", "CTBN.JK", "CUAN.JK",
    "CYBR.JK", "DAAZ.JK", "DADA.JK", "DATA.JK", "DAYA.JK", "DEFI.JK", "DEPO.JK",
    "DEWI.JK", "DFAM.JK", "DGIK.JK", "DGNS.JK", "DGWG.JK", "DIGI.JK", "DIVA.JK",
    "DKHH.JK", "DMMX.JK", "DMND.JK", "DNET.JK", "DOOH.JK", "DPNS.JK", "DPUM.JK",
    "DRMA.JK", "DSFI.JK", "DSSA.JK", "DYAN.JK", "EAST.JK", "ECII.JK", "ELIT.JK",
    "ELPI.JK", "ELTY.JK", "EMAS.JK", "ENAK.JK", "ENZO.JK", "EPAC.JK", "EPMT.JK",
    "ERAL.JK", "ERTX.JK", "ESTA.JK", "ESTI.JK", "EURO.JK", "FAPA.JK", "FILM.JK",
    "FIMP.JK", "FIRE.JK", "FISH.JK", "FITT.JK", "FLMC.JK", "FOLK.JK", "FOOD.JK",
    "FORE.JK", "FORU.JK", "FPNI.JK", "FUJI.JK", "FWCT.JK", "GDST.JK", "GEMA.JK",
    "GGRM.JK", "GGRP.JK", "GLVA.JK", "GMFI.JK", "GOLD.JK", "GOLF.JK", "GPSO.JK",
    "GRIA.JK", "GRPH.JK", "GRPM.JK", "GSMF.JK", "GTRA.JK", "GULA.JK", "GUNA.JK",
    "HAIS.JK", "HAJJ.JK", "HALO.JK", "HATM.JK", "HBAT.JK", "HDFA.JK", "HDIT.JK",
    "HEXA.JK", "HGII.JK", "HILL.JK", "HOMI.JK", "HOPE.JK", "HRME.JK", "HUMI.JK",
    "HYGN.JK", "IATA.JK", "IBFN.JK", "IBOS.JK", "ICON.JK", "IDEA.JK", "IFII.JK",
    "IFSH.JK", "IKAI.JK", "IKAN.JK", "IKBI.JK", "IKPM.JK", "IMAS.JK", "IMJS.JK",
    "INCF.JK", "INCI.JK", "INDO.JK", "INDR.JK", "INDX.JK", "INOV.JK", "INPS.JK",
    "INTA.JK", "INTD.JK", "IOTF.JK", "IPAC.JK", "IPCC.JK", "IPPE.JK", "IPTV.JK",
    "IRRA.JK", "IRSX.JK", "ISAP.JK", "ISEA.JK", "ITIC.JK", "JARR.JK", "JAST.JK",
    "JATI.JK", "JAWA.JK", "JAYA.JK", "JGLE.JK", "JIHD.JK", "JKON.JK", "JMAS.JK",
    "JSPT.JK", "JTPE.JK", "KAEF.JK", "KAQI.JK", "KARW.JK", "KBAG.JK", "KBLV.JK",
    "KDTN.JK", "KEEN.JK", "KETR.JK", "KICI.JK", "KING.JK", "KINO.JK", "KIOS.JK",
    "KJEN.JK", "KKES.JK", "KLAS.JK", "KLIN.JK", "KMDS.JK", "KMTR.JK", "KOBX.JK",
    "KOCI.JK", "KOKA.JK", "KONI.JK", "KOTA.JK", "KRAS.JK", "KREN.JK", "KRYA.JK",
    "KSIX.JK", "KUAS.JK", "LABA.JK", "LABS.JK", "LAJU.JK", "LAND.JK", "LAPD.JK",
    "LCKM.JK", "LFLO.JK", "LIFE.JK", "LIVE.JK", "LMAX.JK", "LMPI.JK", "LOPI.JK",
    "LPIN.JK", "LPLI.JK", "LPPS.JK", "LUCY.JK", "MAHA.JK", "MAIN.JK", "MANG.JK",
    "MAPA.JK", "MAPB.JK", "MARI.JK", "MARK.JK", "MASB.JK", "MAXI.JK", "MAYA.JK",
    "MBMA.JK", "MBTO.JK", "MCOL.JK", "MCOR.JK", "MDIA.JK", "MDIY.JK", "MDKI.JK",
    "MDLA.JK", "MDRN.JK", "MEDS.JK", "MEGA.JK", "MEJA.JK", "MENN.JK", "MERI.JK",
    "MGLV.JK", "MGNA.JK", "MGRO.JK", "MICE.JK", "MIDI.JK", "MINA.JK", "MINE.JK",
    "MKAP.JK", "MKNT.JK", "MKTR.JK", "MLPL.JK", "MMIX.JK", "MMLP.JK", "MOLI.JK",
    "MPIX.JK", "MPRO.JK", "MPXL.JK", "MRAT.JK", "MSIE.JK", "MSIN.JK", "MSJA.JK",
    "MSKY.JK", "MSTI.JK", "MTEL.JK", "MTMH.JK", "MTPS.JK", "MTWI.JK", "MUTU.JK",
    "MYTX.JK", "NAIK.JK", "NANO.JK", "NASA.JK", "NASI.JK", "NATO.JK", "NAYZ.JK",
    "NCKL.JK", "NEST.JK", "NETV.JK", "NICE.JK", "NICK.JK", "NICL.JK", "NINE.JK",
    "NPGF.JK", "NSSS.JK", "NTBK.JK", "NZIA.JK", "OASA.JK", "OBAT.JK", "OBMD.JK",
    "OILS.JK", "OKAS.JK", "OLIV.JK", "OMED.JK", "OPMS.JK", "PACK.JK", "PADA.JK",
    "PAMG.JK", "PANI.JK", "PANR.JK", "PART.JK", "PBRX.JK", "PBSA.JK", "PDES.JK",
    "PDPP.JK", "PEVE.JK", "PGJO.JK", "PGUN.JK", "PICO.JK", "PIPA.JK", "PJHB.JK",
    "PLAN.JK", "PLIN.JK", "PMJS.JK", "PMMP.JK", "PMUI.JK", "PNBN.JK", "PNSE.JK",
    "POLA.JK", "POLI.JK", "POLU.JK", "POLY.JK", "PPGL.JK", "PPRE.JK", "PPRI.JK",
    "PRAY.JK", "PRDA.JK", "PRIM.JK", "PSAT.JK", "PSGO.JK", "PSKT.JK", "PTDU.JK",
    "PTMP.JK", "PTMR.JK", "PTPS.JK", "PTPW.JK", "PTSP.JK", "PUDP.JK", "PURA.JK",
    "PURI.JK", "PZZA.JK", "RAAM.JK", "RAFI.JK", "RALS.JK", "RATU.JK", "RBMS.JK",
    "RCCC.JK", "REAL.JK", "RELF.JK", "RGAS.JK", "RISE.JK", "RLCO.JK", "RMKE.JK",
    "RMKO.JK", "ROCK.JK", "RONY.JK", "RSCH.JK", "RSGK.JK", "RUNS.JK", "SAGE.JK",
    "SAMF.JK", "SAPX.JK", "SATU.JK", "SBMA.JK", "SCCO.JK", "SCNP.JK", "SDPC.JK",
    "SEMA.JK", "SICO.JK", "SILO.JK", "SINI.JK", "SKBM.JK", "SKRN.JK", "SLIS.JK",
    "SMBR.JK", "SMGA.JK", "SMIL.JK", "SMKL.JK", "SMKM.JK", "SMLE.JK", "SMMA.JK",
    "SMSM.JK", "SNLK.JK", "SOFA.JK", "SOLA.JK", "SONA.JK", "SOSS.JK", "SOTS.JK",
    "SOUL.JK", "SPMA.JK", "SPRE.JK", "SPTO.JK", "SQMI.JK", "SRAJ.JK", "SSIA.JK",
    "STAA.JK", "STAR.JK", "STRK.JK", "STTP.JK", "SUNI.JK", "SUPA.JK", "SUPR.JK",
    "SURI.JK", "SWAT.JK", "SWID.JK", "TALF.JK", "TAYS.JK", "TBMS.JK", "TCID.JK",
    "TCPI.JK", "TEBE.JK", "TFAS.JK", "TGKA.JK", "TGRA.JK", "TGUK.JK", "TIFA.JK",
    "TINS.JK", "TIRA.JK", "TLDN.JK", "TMPO.JK", "TNCA.JK", "TOOL.JK", "TOPS.JK",
    "TOSK.JK", "TPIA.JK", "TRGU.JK", "TRIN.JK", "TRIS.JK", "TRON.JK", "TRUE.JK",
    "TRUK.JK", "TRUS.JK", "TYRE.JK", "UANG.JK", "UCID.JK", "UDNG.JK", "UFOE.JK",
    "UNIQ.JK", "UNTD.JK", "UVCR.JK", "VAST.JK", "VERN.JK", "VICO.JK", "VISI.JK",
    "VIVA.JK", "VTNY.JK", "WAPO.JK", "WBSA.JK", "WEHA.JK", "WGSH.JK", "WICO.JK",
    "WIDI.JK", "WINE.JK", "WINR.JK", "WINS.JK", "WMPP.JK", "WOMF.JK", "WOWS.JK",
    "XCIS.JK", "YOII.JK", "YULE.JK", "YUPI.JK", "ZATA.JK", "ZBRA.JK", "ZONE.JK",
    "ZYRX.JK"
]

# =============================================================
# HELPERS
# =============================================================
def log(msg):
    print(msg, flush=True)


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def load_tickers(path):
    if not os.path.exists(path):
        folder = os.path.dirname(path) or "."
        isi = os.listdir(folder) if os.path.isdir(folder) else ["<folder tidak ada>"]
        raise FileNotFoundError(
            f"File ticker tidak ditemukan: {path}\nIsi folder: {isi}"
        )
    ext = os.path.splitext(path)[1].lower()
    if ext in (".txt",):
        with open(path, "r") as f:
            raw = [ln.strip() for ln in f if ln.strip()]
    elif ext == ".csv":
        df = pd.read_csv(path, header=None)
        raw = df.iloc[:, 0].astype(str).tolist()
    elif ext in (".xlsx", ".xls"):
        df = pd.read_excel(path, header=None)
        raw = df.iloc[:, 0].astype(str).tolist()
    else:
        raise ValueError(f"Format ticker tidak didukung: {ext}")

    tickers = []
    for t in raw:
        t = str(t).strip().upper().replace(" ", "")
        if not t or t.startswith("#") or t.startswith("IDX_TICKERS"):
            continue
        t = t.replace("[", "").replace("]", "").replace('"', "").replace("'", "").replace(",", "")
        if not t:
            continue
        if t.endswith(".JK"):
            tick = t
        elif t.isalpha() and 2 <= len(t) <= 6:
            tick = t + ".JK"
        else:
            continue
        tickers.append(tick)

    seen = set()
    uniq = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            uniq.append(t)
    return uniq


def safe_last(series):
    try:
        s = series.dropna()
        if len(s) == 0:
            return np.nan
        return float(s.iloc[-1])
    except Exception:
        return np.nan


def _fix_columns(df):
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    return df


def _tz_naive_index(df):
    if df is None or df.empty:
        return df
    idx = df.index
    if isinstance(idx, pd.DatetimeIndex) and idx.tz is not None:
        df = df.copy()
        df.index = idx.tz_localize(None)
    return df


def _safe_float(x):
    """Konversi nilai ke float; NaN jika gagal/None/NaN/Inf."""
    try:
        if x is None:
            return np.nan
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return np.nan
        return v
    except Exception:
        return np.nan


def _empty_table():
    return pd.DataFrame(
        columns=["Nama Perusahaan", "Kode Saham", "Parameter",
                 "Value", "Kriteria", "Standard yang Dipakai"]
    )


# =============================================================
# MODUL DATA HARGA
# =============================================================
def download_prices(tickers, cfg):
    data = {}
    batch_size = cfg["BATCH_SIZE"]
    retry = cfg["RETRY"]
    batches = [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]

    for bi, batch in enumerate(batches, 1):
        log(f"[HARGA] batch {bi}/{len(batches)} ({len(batch)} ticker)")
        for attempt in range(retry + 1):
            try:
                raw = yf.download(
                    batch, period=cfg["PERIOD"], interval=cfg["INTERVAL"],
                    auto_adjust=True, group_by="ticker", threads=True, progress=False,
                )
                if raw is None or raw.empty:
                    continue
                if isinstance(raw.columns, pd.MultiIndex):
                    level0 = raw.columns.get_level_values(0).unique().tolist()
                    for t in batch:
                        if t in level0:
                            sub = raw[t]
                            sub = _fix_columns(sub)
                            sub = _tz_naive_index(sub)
                            sub = sub.dropna(how="all")
                            if len(sub) > 0:
                                data[t] = sub
                else:
                    if len(batch) == 1:
                        sub = _fix_columns(raw)
                        sub = _tz_naive_index(sub)
                        sub = sub.dropna(how="all")
                        if len(sub) > 0:
                            data[batch[0]] = sub
                break
            except Exception as e:
                if attempt < retry:
                    time.sleep(1.5)
                else:
                    log(f"  gagal batch {bi}: {e}")

    dropped = []
    valid = {}
    for t, df in data.items():
        if df is None or df.empty or len(df) < cfg["MIN_BARS"]:
            dropped.append(t)
        elif "Close" not in df.columns or "Volume" not in df.columns:
            dropped.append(t)
        else:
            valid[t] = df

    log(f"[HARGA] diterima: {len(valid)}, dihapus (data kurang): {len(dropped)}")
    if dropped:
        log(f"[HARGA] dihapus: {dropped}")
    return valid


# =============================================================
# INDIKATOR TEKNIKAL
# =============================================================
def _rsi_wilder(close, period=14):
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - 100 / (1 + rs)
    return rsi


def _stoch_slow(high, low, close, k_period=14, slow=3, d_period=3):
    ll = low.rolling(k_period).min()
    hh = high.rolling(k_period).max()
    rng = (hh - ll).replace(0, np.nan)
    raw_k = 100 * (close - ll) / rng
    k = raw_k.rolling(slow).mean()
    d = k.rolling(d_period).mean()
    return k, d


def compute_technicals(df, cfg):
    try:
        close = df["Close"].astype(float)
        high = df["High"].astype(float)
        low = df["Low"].astype(float)
        vol = df["Volume"].astype(float)

        rsi = _rsi_wilder(close, cfg["RSI_PERIOD"])
        k, d = _stoch_slow(high, low, close, cfg["STOCH_K"], cfg["STOCH_SLOW"], cfg["STOCH_D"])

        res = {
            "close": safe_last(close),
            "rsi14": safe_last(rsi),
            "stoch_k": safe_last(k),
            "stoch_d": safe_last(d),
        }

        for p in cfg["MA_PERIODS"]:
            res[f"ma{p}"] = safe_last(close.rolling(p).mean())

        h52 = high.tail(cfg["HIGH52_BARS"]).max()
        l52 = low.tail(cfg["HIGH52_BARS"]).min()
        res["high52"] = float(h52) if pd.notna(h52) else np.nan
        res["low52"] = float(l52) if pd.notna(l52) else np.nan

        c = res["close"]
        if pd.notna(c) and pd.notna(res["high52"]) and res["high52"] > 0:
            res["drawdown_52w"] = (res["high52"] - c) / res["high52"] * 100
        else:
            res["drawdown_52w"] = np.nan

        if pd.notna(c) and pd.notna(res["ma200"]) and res["ma200"] != 0:
            res["jarak_ma200"] = (c / res["ma200"] - 1) * 100
        else:
            res["jarak_ma200"] = np.nan

        val = (close * vol).tail(cfg["VALUE_AVG_DAYS"])
        res["avg_value_20d"] = float(val.mean()) if len(val) > 0 else np.nan

        res["n_bars"] = len(close)
        return res
    except Exception as e:
        return {"error": str(e)}


# =============================================================
# DATA FUNDAMENTAL
# =============================================================
def _safe_info(ticker_obj):
    """Ambil info yfinance dengan aman."""
    try:
        info = ticker_obj.info
        return info if isinstance(info, dict) else {}
    except Exception:
        return {}


def _safe_fast_info_last_price(ticker_obj):
    """Ambil last price dari fast_info dengan aman (FastInfo bukan dict)."""
    try:
        fi = ticker_obj.fast_info
    except Exception:
        return np.nan
    # Coba beberapa cara akses
    for attr in ("last_price", "lastPrice", "regularMarketPrice"):
        try:
            v = getattr(fi, attr, None)
            if v is not None:
                f = _safe_float(v)
                if pd.notna(f) and f > 0:
                    return f
        except Exception:
            continue
    try:
        d = dict(fi)
        for key in ("last_price", "lastPrice", "regularMarketPrice"):
            if key in d:
                f = _safe_float(d[key])
                if pd.notna(f) and f > 0:
                    return f
    except Exception:
        pass
    return np.nan


def _fund_from_info(ticker):
    t = yf.Ticker(ticker)
    out = {"ticker": ticker}

    info = _safe_info(t)
    name = info.get("longName") or info.get("shortName") or np.nan
    out["nama"] = name

    roe_v = _safe_float(info.get("returnOnEquity"))
    out["roe_pct"] = roe_v * 100 if pd.notna(roe_v) else np.nan

    der_v = _safe_float(info.get("debtToEquity"))
    out["der"] = der_v / 100.0 if pd.notna(der_v) else np.nan

    mc_v = _safe_float(info.get("marketCap"))
    out["market_cap_t"] = mc_v / 1e12 if pd.notna(mc_v) else np.nan

    # --- fallback DER & ROE via balance sheet ---
    try:
        bs = t.balance_sheet
        if bs is not None and not bs.empty:
            if pd.isna(out["der"]):
                td, se = None, None
                if "Total Debt" in bs.index:
                    td = bs.loc["Total Debt"].iloc[0]
                for lbl in ["Stockholders Equity", "Total Stockholder Equity", "Common Stock Equity"]:
                    if lbl in bs.index:
                        se = bs.loc[lbl].iloc[0]
                        break
                if td is not None and se is not None and pd.notna(se) and se != 0:
                    out["der"] = float(td) / float(se)
            if pd.isna(out["roe_pct"]):
                ni, se = None, None
                for lbl in ["Net Income", "Net Income Common Stockholders"]:
                    if lbl in bs.index:
                        ni = bs.loc[lbl].iloc[0]
                        break
                for lbl in ["Stockholders Equity", "Total Stockholder Equity", "Common Stock Equity"]:
                    if lbl in bs.index:
                        se = bs.loc[lbl].iloc[0]
                        break
                if ni is not None and se is not None and pd.notna(se) and se != 0:
                    out["roe_pct"] = float(ni) / float(se) * 100
    except Exception:
        pass

    # --- Growth YoY: net income dulu, fallback earningsGrowth ---
    growth = np.nan
    try:
        inc = t.income_stmt
        if inc is not None and not inc.empty:
            ni_row = None
            for lbl in ["Net Income", "Net Income Common Stockholders"]:
                if lbl in inc.index:
                    ni_row = inc.loc[lbl]
                    break
            if ni_row is not None and len(ni_row.dropna()) >= 2:
                s = ni_row.dropna()
                latest = float(s.iloc[0])
                prev = float(s.iloc[1])
                if prev > 0:
                    growth = (latest / prev - 1) * 100
    except Exception:
        pass

    if pd.isna(growth):
        eg = _safe_float(info.get("earningsGrowth"))
        if pd.notna(eg):
            growth = eg * 100
            out["growth_source"] = "earningsGrowth(info)"
        else:
            out["growth_source"] = "N/A"
    else:
        out["growth_source"] = "net_income_yoy"

    out["growth_yoy_pct"] = growth

    # --- Dividend Yield: cutoff absolut 365 hari dari sekarang ---
    try:
        divs = t.dividends
        if divs is not None and len(divs) > 0:
            divs = divs.copy()
            if getattr(divs.index, "tz", None) is not None:
                divs.index = divs.index.tz_localize(None)
            cutoff = pd.Timestamp.now() - pd.Timedelta(days=365)
            total_div = float(divs[divs.index >= cutoff].sum())

            last_close = np.nan
            try:
                hist = t.history(period="5d", auto_adjust=True)
                if hist is not None and not hist.empty:
                    last_close = float(hist["Close"].iloc[-1])
            except Exception:
                pass
            if pd.isna(last_close):
                last_close = _safe_fast_info_last_price(t)
            if pd.isna(last_close):
                last_close = _safe_float(info.get("regularMarketPrice"))

            if pd.notna(last_close) and last_close > 0:
                dy = total_div / last_close * 100
                out["div_yield_pct"] = dy
                if dy > 12.0:
                    log(f"  [WARN-DIV] {ticker}: dividend yield ekstrem {dy:.2f}% "
                        f"(total div 12 bln = {total_div:.2f}, close = {last_close:.2f}). "
                        f"Verifikasi manual dengan laporan keuangan.")
            else:
                out["div_yield_pct"] = np.nan
        else:
            out["div_yield_pct"] = 0.0
    except Exception:
        out["div_yield_pct"] = np.nan

    return out


def _read_fund_cache(cache_path, cfg):
    if not os.path.exists(cache_path):
        return None
    try:
        mtime = os.path.getmtime(cache_path)
        age_days = (time.time() - mtime) / 86400.0
        if age_days > cfg["FUND_CACHE_MAX_AGE_DAYS"]:
            log(f"[FUND] cache ditemukan tapi berumur {age_days:.1f} hari "
                f"(> {cfg['FUND_CACHE_MAX_AGE_DAYS']}), diabaikan.")
            return None
        df = pd.read_csv(cache_path)
        if df is None or df.empty:
            return None
        for c in ["ticker", "nama", "roe_pct", "der", "growth_yoy_pct",
                  "div_yield_pct", "market_cap_t", "growth_source"]:
            if c not in df.columns:
                df[c] = np.nan
        df["ticker"] = df["ticker"].astype(str)
        log(f"[FUND] cache dibaca: {len(df)} baris (umur {age_days:.1f} hari).")
        return df
    except Exception as e:
        log(f"[FUND] gagal membaca cache: {e}")
        return None


def download_fundamentals(tickers, cfg):
    ensure_dir(cfg["CACHE_DIR"])
    cache_path = os.path.join(cfg["CACHE_DIR"], "fundamental_cache.csv")

    cached = _read_fund_cache(cache_path, cfg)
    if cached is not None:
        cached_tickers = set(cached["ticker"].tolist())
        missing = [t for t in tickers if t not in cached_tickers]
        if not missing:
            log(f"[FUND] seluruh {len(tickers)} ticker tersedia di cache, skip download.")
            return cached
        log(f"[FUND] cache coverage: {len(cached_tickers)}/{len(tickers)}, "
            f"fetch {len(missing)} ticker tambahan.")
    else:
        cached = None
        missing = list(tickers)

    results = []
    log(f"[FUND] mengambil fundamental {len(missing)} ticker "
        f"(workers={cfg['FUND_WORKERS']}, retry={cfg['FUND_RETRY']})")

    def worker(tk):
        last_err = None
        for attempt in range(cfg["FUND_RETRY"] + 1):
            try:
                res = _fund_from_info(tk)
                metrics = [res.get("roe_pct"), res.get("der"), res.get("market_cap_t"),
                           res.get("growth_yoy_pct"), res.get("div_yield_pct")]
                if pd.notna(res.get("nama")):
                    return res
                if any(pd.notna(m) for m in metrics):
                    return res
                last_err = "all metrics NaN"
            except Exception as e:
                last_err = str(e)
            if attempt < cfg["FUND_RETRY"]:
                time.sleep(0.8 * (attempt + 1))
        log(f"  [FUND] {tk}: gagal setelah {cfg['FUND_RETRY']+1} percobaan ({last_err})")
        return {"ticker": tk, "nama": np.nan, "roe_pct": np.nan, "der": np.nan,
                "growth_yoy_pct": np.nan, "div_yield_pct": np.nan,
                "market_cap_t": np.nan, "growth_source": "N/A"}

    with ThreadPoolExecutor(max_workers=cfg["FUND_WORKERS"]) as ex:
        futs = {ex.submit(worker, t): t for t in missing}
        done = 0
        for fut in as_completed(futs):
            try:
                results.append(fut.result())
            except Exception as e:
                log(f"  [FUND] future error {futs[fut]}: {e}")
                results.append({"ticker": futs[fut], "nama": np.nan, "roe_pct": np.nan,
                                "der": np.nan, "growth_yoy_pct": np.nan,
                                "div_yield_pct": np.nan, "market_cap_t": np.nan,
                                "growth_source": "N/A"})
            done += 1
            if done % 25 == 0:
                log(f"  [FUND] {done}/{len(missing)}")

    new_df = pd.DataFrame(results)
    if cached is not None and not cached.empty:
        df = pd.concat([cached, new_df], ignore_index=True)
        df = df.drop_duplicates(subset=["ticker"], keep="last")
    else:
        df = new_df

    try:
        df.to_csv(cache_path, index=False)
    except Exception as e:
        log(f"[FUND] gagal menulis cache: {e}")

    cov = {c: int(df[c].notna().sum()) for c in
           ["roe_pct", "der", "growth_yoy_pct", "div_yield_pct", "market_cap_t"]}
    log(f"[FUND] coverage: {cov}")
    return df


# =============================================================
# GABUNG MASTER
# =============================================================
def build_master(price_data, fund_df, cfg):
    rows = []
    for tk, df in price_data.items():
        tech = compute_technicals(df, cfg)
        if "error" in tech:
            log(f"  [TEKNIKAL] gagal {tk}: {tech['error']}")
            continue
        row = {"ticker": tk}
        row.update(tech)
        rows.append(row)
    tech_df = pd.DataFrame(rows)

    # Pastikan fund_df punya kolom yang diperlukan
    if fund_df is None or fund_df.empty:
        fund_df = pd.DataFrame(columns=["ticker", "nama", "roe_pct", "der",
                                        "growth_yoy_pct", "div_yield_pct",
                                        "market_cap_t", "growth_source"])
    for c in ["nama", "roe_pct", "der", "growth_yoy_pct",
              "div_yield_pct", "market_cap_t", "growth_source"]:
        if c not in fund_df.columns:
            fund_df[c] = np.nan

    master = tech_df.merge(
        fund_df[["ticker", "nama", "roe_pct", "der", "growth_yoy_pct",
                 "div_yield_pct", "market_cap_t", "growth_source"]],
        on="ticker", how="left"
    )

    cols = ["ticker", "nama", "close", "rsi14", "stoch_k", "stoch_d",
            "ma50", "ma100", "ma200", "high52", "low52",
            "drawdown_52w", "jarak_ma200", "avg_value_20d", "n_bars",
            "roe_pct", "der", "growth_yoy_pct", "div_yield_pct", "market_cap_t",
            "growth_source"]
    for c in cols:
        if c not in master.columns:
            master[c] = np.nan
    return master[cols]


# =============================================================
# MODUL 1 - FILTER KUALITAS
# =============================================================
def modul_1(master, cfg):
    log("\n" + "=" * 70)
    log("MODUL 1 - FILTER KUALITAS SAHAM")
    log("=" * 70)
    df = master.copy()
    n_in = len(df)

    def crit_roe(v):
        return pd.notna(v) and v > cfg["M1_ROE_MIN"]

    def crit_der(v):
        return pd.notna(v) and v < cfg["M1_DER_MAX"]

    def crit_growth(v):
        return pd.notna(v) and v > cfg["M1_GROWTH_MIN"]

    def crit_div(v):
        return pd.notna(v) and v > cfg["M1_DIV_MIN"]

    def crit_mc(v):
        return pd.notna(v) and v > cfg["M1_MKTCAP_MIN_T"]

    def crit_liq(v):
        return pd.notna(v) and v > cfg["M1_VALUE_MIN_M"] * 1e9

    df["pass_roe"] = df["roe_pct"].apply(crit_roe)
    df["pass_der"] = df["der"].apply(crit_der)
    df["pass_growth"] = df["growth_yoy_pct"].apply(crit_growth)
    df["pass_div"] = df["div_yield_pct"].apply(crit_div)
    df["pass_mc"] = df["market_cap_t"].apply(crit_mc)

    base_criteria = ["pass_roe", "pass_der", "pass_growth", "pass_div", "pass_mc"]
    if cfg["M1_FILTER_LIQUIDITY"]:
        df["pass_liq"] = df["avg_value_20d"].apply(crit_liq)
        base_criteria.append("pass_liq")
    else:
        df["pass_liq"] = True

    df["pass_count"] = df[base_criteria].sum(axis=1)
    df["lolos"] = df["pass_count"] == len(base_criteria)

    df["ideal_count"] = (
        (df["roe_pct"] >= cfg["M1_ROE_IDEAL"]).astype(int)
        + (df["der"] < cfg["M1_DER_IDEAL"]).astype(int)
        + (df["market_cap_t"] >= cfg["M1_MKTCAP_IDEAL_T"]).astype(int)
    )
    df["star"] = df["ideal_count"].apply(lambda x: "★" if x == 3 else ("☆" if x >= 1 else ""))

    # --- Diagnostik ---
    crit_map = {
        "ROE": ("roe_pct", "pass_roe"),
        "DER": ("der", "pass_der"),
        "GROWTH": ("growth_yoy_pct", "pass_growth"),
        "DIV": ("div_yield_pct", "pass_div"),
        "MCAP": ("market_cap_t", "pass_mc"),
    }
    if cfg["M1_FILTER_LIQUIDITY"]:
        crit_map["LIQ"] = ("avg_value_20d", "pass_liq")

    log("[M1] Diagnostik kriteria (NaN vs tidak memenuhi batas):")
    for name, (col, flag) in crit_map.items():
        nan_c = int(df[col].isna().sum())
        fail_c = int((df[col].notna() & (~df[flag])).sum())
        log(f"    {name:6s} : NaN={nan_c:4d}  |  di luar batas={fail_c:4d}")

    passed = df[df["lolos"]].copy()
    passed = passed.sort_values(["ideal_count", "roe_pct"], ascending=[False, False])
    passed = passed.head(cfg["M1_MAX_OUT"])

    rows = []
    for _, r in passed.iterrows():
        nm = r["nama"] if pd.notna(r["nama"]) else r["ticker"]
        tk = r["ticker"]
        roe = r["roe_pct"]
        der = r["der"]
        gro = r["growth_yoy_pct"]
        div = r["div_yield_pct"]
        mc = r["market_cap_t"]
        rows.append({"Nama Perusahaan": nm, "Kode Saham": tk, "Parameter": "ROE",
                     "Value": f"{roe:.2f}%" if pd.notna(roe) else "N/A",
                     "Kriteria": f"> {cfg['M1_ROE_MIN']}% {'✔' if roe > cfg['M1_ROE_MIN'] else '✘'}",
                     "Standard yang Dipakai": f"ROE ideal >= {cfg['M1_ROE_IDEAL']}% {r['star']}"})
        rows.append({"Nama Perusahaan": nm, "Kode Saham": tk, "Parameter": "DER",
                     "Value": f"{der:.3f}" if pd.notna(der) else "N/A",
                     "Kriteria": f"< {cfg['M1_DER_MAX']} {'✔' if pd.notna(der) and der < cfg['M1_DER_MAX'] else '✘'}",
                     "Standard yang Dipakai": f"DER ideal < {cfg['M1_DER_IDEAL']}"})
        rows.append({"Nama Perusahaan": nm, "Kode Saham": tk, "Parameter": "Net Profit Growth YoY",
                     "Value": f"{gro:.2f}%" if pd.notna(gro) else "N/A",
                     "Kriteria": f"> {cfg['M1_GROWTH_MIN']}% {'✔' if pd.notna(gro) and gro > cfg['M1_GROWTH_MIN'] else '✘'}",
                     "Standard yang Dipakai": "Pertumbuhan laba positif"})
        rows.append({"Nama Perusahaan": nm, "Kode Saham": tk, "Parameter": "Dividend Yield",
                     "Value": f"{div:.2f}%" if pd.notna(div) else "N/A",
                     "Kriteria": f"> {cfg['M1_DIV_MIN']}% {'✔' if pd.notna(div) and div > cfg['M1_DIV_MIN'] else '✘'}",
                     "Standard yang Dipakai": f"Dividend yield > {cfg['M1_DIV_MIN']}%"})
        rows.append({"Nama Perusahaan": nm, "Kode Saham": tk, "Parameter": "Market Cap",
                     "Value": f"Rp {mc:.2f} Triliun" if pd.notna(mc) else "N/A",
                     "Kriteria": f"> Rp {cfg['M1_MKTCAP_MIN_T']} T {'✔' if pd.notna(mc) and mc > cfg['M1_MKTCAP_MIN_T'] else '✘'}",
                     "Standard yang Dipakai": f"Market cap ideal >= Rp {cfg['M1_MKTCAP_IDEAL_T']} T"})
        if cfg["M1_FILTER_LIQUIDITY"]:
            liq = r["avg_value_20d"]
            rows.append({"Nama Perusahaan": nm, "Kode Saham": tk, "Parameter": "Likuiditas Harian",
                         "Value": f"Rp {liq/1e9:.2f} Miliar" if pd.notna(liq) else "N/A",
                         "Kriteria": f"> Rp {cfg['M1_VALUE_MIN_M']} M {'✔' if r['pass_liq'] else '✘'}",
                         "Standard yang Dipakai": f"> Rp {cfg['M1_VALUE_MIN_M']} Miliar/hari"})

    out_df = pd.DataFrame(rows) if rows else _empty_table()
    log(f"diproses -> {n_in} saham, lolos -> {len(passed)}")
    if len(out_df) > 0:
        with pd.option_context("display.max_rows", None, "display.max_columns", None,
                               "display.width", None, "display.max_colwidth", None):
            print(out_df.to_string(index=False))
    else:
        log("(tidak ada saham yang lolos)")
    return passed, out_df


# =============================================================
# MODUL 2 - OVERSOLD
# =============================================================
def modul_2(passed1, cfg):
    log("\n" + "=" * 70)
    log("MODUL 2 - KONDISI OVERSOLD")
    log("=" * 70)
    n_in = len(passed1)
    if n_in == 0:
        log("diproses -> 0 saham, lolos -> 0")
        return passed1.iloc[0:0].copy(), _empty_table()

    df = passed1.copy()

    def c_rsi(v):
        return pd.notna(v) and v < cfg["M2_RSI_MAX"]

    def c_ma(v_close, v_ma):
        return pd.notna(v_close) and pd.notna(v_ma) and v_close <= v_ma * cfg["M2_MA200_TOL"]

    def c_dd(v):
        return pd.notna(v) and cfg["M2_DD_MIN"] <= v <= cfg["M2_DD_MAX"]

    def c_stoch(v):
        return pd.notna(v) and v < cfg["M2_STOCH_MAX"]

    df["c_rsi"] = df["rsi14"].apply(c_rsi)
    df["c_ma"] = df.apply(lambda r: c_ma(r["close"], r["ma200"]), axis=1)
    df["c_dd"] = df["drawdown_52w"].apply(c_dd)
    df["c_stoch"] = df["stoch_k"].apply(c_stoch)
    df["m2_count"] = df[["c_rsi", "c_ma", "c_dd", "c_stoch"]].sum(axis=1)
    df["m2_pass"] = df["m2_count"] >= cfg["M2_MIN_PASS"]

    out = df[df["m2_pass"]].copy()
    out = out.sort_values(["m2_count", "rsi14"], ascending=[False, True]).head(cfg["M2_MAX_OUT"])

    rows = []
    for _, r in out.iterrows():
        nm = r["nama"] if pd.notna(r["nama"]) else r["ticker"]
        tk = r["ticker"]
        rows.append({"Nama Perusahaan": nm, "Kode Saham": tk, "Parameter": "RSI14",
                     "Value": f"{r['rsi14']:.2f}" if pd.notna(r['rsi14']) else "N/A",
                     "Kriteria": f"< {cfg['M2_RSI_MAX']} {'✔' if r['c_rsi'] else '✘'}",
                     "Standard yang Dipakai": f"RSI oversold < {cfg['M2_RSI_MAX']}"})
        rows.append({"Nama Perusahaan": nm, "Kode Saham": tk, "Parameter": "Close vs MA200",
                     "Value": f"{r['close']:.0f} vs {r['ma200']:.0f}" if pd.notna(r['close']) and pd.notna(r['ma200']) else "N/A",
                     "Kriteria": f"<= MA200*{cfg['M2_MA200_TOL']} {'✔' if r['c_ma'] else '✘'}",
                     "Standard yang Dipakai": "Harga di bawah/sekitar MA200"})
        rows.append({"Nama Perusahaan": nm, "Kode Saham": tk, "Parameter": "Drawdown 52w",
                     "Value": f"{r['drawdown_52w']:.2f}%" if pd.notna(r['drawdown_52w']) else "N/A",
                     "Kriteria": f"{cfg['M2_DD_MIN']}% - {cfg['M2_DD_MAX']}% {'✔' if r['c_dd'] else '✘'}",
                     "Standard yang Dipakai": "Koreksi wajar 15-35%"})
        rows.append({"Nama Perusahaan": nm, "Kode Saham": tk, "Parameter": "Stochastic %K",
                     "Value": f"{r['stoch_k']:.2f}" if pd.notna(r['stoch_k']) else "N/A",
                     "Kriteria": f"< {cfg['M2_STOCH_MAX']} {'✔' if r['c_stoch'] else '✘'}",
                     "Standard yang Dipakai": f"Stoch %K < {cfg['M2_STOCH_MAX']}"})

    out_df = pd.DataFrame(rows) if rows else _empty_table()
    log(f"diproses -> {n_in} saham, lolos -> {len(out)}")
    if len(out_df) > 0:
        with pd.option_context("display.max_rows", None, "display.max_columns", None,
                               "display.width", None, "display.max_colwidth", None):
            print(out_df.to_string(index=False))
    else:
        log("(tidak ada saham yang lolos)")
    return out, out_df


# =============================================================
# MODUL 3 - SUPPORT
# =============================================================
def _pivot_lows(low, window):
    vals = low.values
    n = len(vals)
    pivots = []
    for i in range(window, n - window):
        seg = vals[i - window:i + window + 1]
        if vals[i] == np.min(seg) and np.isfinite(vals[i]):
            pivots.append(float(vals[i]))
    return pivots


def _cluster_levels(pivots, tol):
    if not pivots:
        return []
    pivots = sorted(pivots)
    clusters = []
    cur = [pivots[0]]
    for p in pivots[1:]:
        if (p - cur[-1]) / cur[-1] <= tol:
            cur.append(p)
        else:
            clusters.append(cur)
            cur = [p]
    clusters.append(cur)
    return [float(np.mean(c)) for c in clusters]


def _count_touches(low, level, tol):
    return int((np.abs(low - level) / level <= tol).sum())


def _support_info(tk, df, cfg):
    close = float(df["Close"].iloc[-1])
    high = df["High"].astype(float)
    low = df["Low"].astype(float)
    vol = df["Volume"].astype(float)
    ma50 = safe_last(df["Close"].rolling(50).mean())
    ma100 = safe_last(df["Close"].rolling(100).mean())

    res = {
        "close": close,
        "support1": np.nan,
        "support_major": np.nan,
        "jenis": [],
        "detail": {},
    }

    near_min = cfg["M3_NEAR_MIN"]
    near_max = cfg["M3_NEAR_MAX"]

    def near_level(level):
        if pd.isna(level) or level <= 0:
            return False
        ratio = close / level
        return near_min <= ratio <= near_max

    def inside_zone(lo, hi):
        if pd.isna(lo) or pd.isna(hi):
            return False
        return lo <= close <= hi

    # a. horizontal
    pivots = _pivot_lows(low, cfg["M3_PIVOT_WINDOW"])
    levels = _cluster_levels(pivots, cfg["M3_CLUSTER_TOL"])
    strong = []
    for lv in levels:
        touches = _count_touches(low.values, lv, cfg["M3_TOUCH_TOL"])
        if touches >= cfg["M3_MIN_TOUCH"]:
            strong.append({"level": lv, "touches": touches})

    # b. dynamic MA
    dyn = []
    for name, ma in [("MA50", ma50), ("MA100", ma100)]:
        if pd.notna(ma) and close >= ma * cfg["M3_DYNAMIC_TOL"]:
            dyn.append({"name": name, "level": ma})

    # c. fibonacci
    h52 = high.tail(cfg["HIGH52_BARS"])
    swing_high = float(h52.max()) if len(h52) > 0 else np.nan
    fib_zone = [np.nan, np.nan]
    fib_hi, fib_lo = np.nan, np.nan
    if pd.notna(swing_high) and swing_high > 0:
        sh_idx = h52.idxmax()
        try:
            pos = df.index.get_loc(sh_idx)
        except KeyError:
            pos = len(df) - 1
        start = max(0, pos - cfg["HIGH52_BARS"] + 1)
        swing_low = float(low.iloc[start:pos + 1].min())
        rng = swing_high - swing_low
        if rng > 0:
            fib_hi = swing_high - 0.5 * rng
            fib_lo = swing_high - 0.618 * rng
            fib_zone = [min(fib_hi, fib_lo), max(fib_hi, fib_lo)]

    # d. demand
    avg_vol20 = vol.rolling(20).mean()
    bullish = (df["Close"] > df["Open"]) & (vol >= cfg["M3_DEMAND_VOL_MULT"] * avg_vol20)
    demand_zones = []
    for i in range(len(df) - 1):
        if bool(bullish.iloc[i]):
            look = min(cfg["M3_DEMAND_LOOKAHEAD"], len(df) - 1 - i)
            if look <= 0:
                continue
            fut = df["Close"].iloc[i + 1:i + 1 + look]
            if len(fut) == 0:
                continue
            rise = (fut.iloc[-1] / df["Close"].iloc[i] - 1) * 100
            if rise >= cfg["M3_DEMAND_RISE_PCT"]:
                zl = float(df["Low"].iloc[i])
                zh = float(df["High"].iloc[i])
                demand_zones.append({"lo": zl, "hi": zh, "idx": i})
    demand_pick = None
    below_close = [z for z in demand_zones if z["hi"] <= close * 1.01]
    if below_close:
        demand_pick = below_close[-1]
    elif demand_zones:
        demand_pick = demand_zones[-1]

    candidates = []
    for s in strong:
        if near_level(s["level"]):
            candidates.append({"type": "horizontal", "level": s["level"], "prio": 1, "touches": s["touches"]})
    for d in dyn:
        if near_level(d["level"]):
            candidates.append({"type": d["name"], "level": d["level"], "prio": 2, "touches": 0})

    fib_mid = (fib_lo + fib_hi) / 2 if pd.notna(fib_hi) and pd.notna(fib_lo) else np.nan
    if pd.notna(fib_hi) and pd.notna(fib_lo):
        if inside_zone(fib_lo, fib_hi):
            candidates.append({"type": "fibonacci", "level": fib_mid, "prio": 3, "touches": 0})
        elif pd.notna(fib_mid) and near_level(fib_mid):
            candidates.append({"type": "fibonacci", "level": fib_mid, "prio": 3, "touches": 0})

    if demand_pick and inside_zone(demand_pick["lo"], demand_pick["hi"]):
        candidates.append({"type": "demand",
                           "level": (demand_pick["lo"] + demand_pick["hi"]) / 2,
                           "prio": 4, "touches": 0})

    if candidates:
        candidates.sort(key=lambda x: (x["prio"], -x["touches"]))
        res["support1"] = candidates[0]["level"]
        res["jenis"] = list({c["type"] for c in candidates})

    res["detail"] = {
        "horizontal_strong": strong,
        "dynamic": dyn,
        "fib_zone": fib_zone,
        "demand": demand_pick,
        "candidates": candidates,
    }

    # --- Support Major (rev6) ---
    ref = res["support1"] if pd.notna(res["support1"]) else close
    max_drop = cfg["M3_SUPPORT_MAJOR_MAX_DROP"]
    lower_bound = ref * (1 - max_drop)
    hm_candidates = [s for s in strong if lower_bound <= s["level"] < ref]
    if hm_candidates:
        hm_candidates.sort(key=lambda s: -s["level"])
        res["support_major"] = hm_candidates[0]["level"]
    else:
        res["support_major"] = float(low.tail(cfg["M3_FALLBACK_LOW_DAYS"]).min())

    return res


def modul_3(passed1, price_data, cfg):
    log("\n" + "=" * 70)
    log("MODUL 3 - DEFINISI SUPPORT")
    log("=" * 70)
    n_in = len(passed1)
    if n_in == 0:
        log("diproses -> 0 saham, lolos -> 0")
        return passed1.iloc[0:0].copy(), _empty_table()

    rows_out = []
    passed_rows = []
    for _, r in passed1.iterrows():
        tk = r["ticker"]
        if tk not in price_data:
            continue
        try:
            info = _support_info(tk, price_data[tk], cfg)
        except Exception as e:
            log(f"  [M3] gagal {tk}: {e}")
            continue

        jenis = info["jenis"]
        close = info["close"]
        if len(jenis) == 0:
            continue

        row = r.to_dict()
        row["support1"] = info["support1"]
        row["support_major"] = info["support_major"]
        row["jenis_support"] = ",".join(jenis)
        row["n_jenis"] = len(jenis)
        passed_rows.append(row)

        nm = r["nama"] if pd.notna(r["nama"]) else tk
        rows_out.append({"Nama Perusahaan": nm, "Kode Saham": tk, "Parameter": "Close",
                         "Value": f"{close:.0f}",
                         "Kriteria": "harga acuan", "Standard yang Dipakai": "-"})
        rows_out.append({"Nama Perusahaan": nm, "Kode Saham": tk, "Parameter": "Support1",
                         "Value": f"{info['support1']:.0f}" if pd.notna(info['support1']) else "N/A",
                         "Kriteria": f"{cfg['M3_NEAR_MIN']:.2f} <= close/level <= {cfg['M3_NEAR_MAX']:.2f}",
                         "Standard yang Dipakai": "level support terdekat (definisi ketat)"})
        rows_out.append({"Nama Perusahaan": nm, "Kode Saham": tk, "Parameter": "Support Major",
                         "Value": f"{info['support_major']:.0f}" if pd.notna(info['support_major']) else "N/A",
                         "Kriteria": f"horizontal <= support1, maks drop {cfg['M3_SUPPORT_MAJOR_MAX_DROP']*100:.0f}%",
                         "Standard yang Dipakai": "horizontal kuat / low 60d"})
        rows_out.append({"Nama Perusahaan": nm, "Kode Saham": tk, "Parameter": "Jenis Support Cocok",
                         "Value": row["jenis_support"],
                         "Kriteria": ">= 1 jenis",
                         "Standard yang Dipakai": "horizontal / MA / Fib / Demand"})

    if passed_rows:
        out = pd.DataFrame(passed_rows)
        out = out.sort_values("n_jenis", ascending=False).head(cfg["M3_MAX_OUT"]).reset_index(drop=True)
        keep = set(out["ticker"].tolist())
        rows_out = [x for x in rows_out if x["Kode Saham"] in keep]
    else:
        out = pd.DataFrame(columns=list(passed1.columns) + ["support1", "support_major", "jenis_support", "n_jenis"])

    out_df = pd.DataFrame(rows_out) if rows_out else _empty_table()
    log(f"diproses -> {n_in} saham, lolos -> {len(out)}")
    if len(out_df) > 0:
        with pd.option_context("display.max_rows", None, "display.max_columns", None,
                               "display.width", None, "display.max_colwidth", None):
            print(out_df.to_string(index=False))
    else:
        log("(tidak ada saham yang lolos)")
    return out, out_df


# =============================================================
# MODUL 4 - AVERAGING
# =============================================================
def modul_4(passed3, cfg):
    log("\n" + "=" * 70)
    log("MODUL 4 - AVERAGING (RENCANA BELI BERTAHAP)")
    log("=" * 70)
    n_in = len(passed3)
    if n_in == 0:
        log("diproses -> 0 saham, lolos -> 0")
        return _empty_table()

    rows = []
    for _, r in passed3.iterrows():
        nm = r["nama"] if pd.notna(r["nama"]) else r["ticker"]
        tk = r["ticker"]
        close = r.get("close", np.nan)
        sup1 = r.get("support1", np.nan)
        sup_maj = r.get("support_major", np.nan)
        rsi = r.get("rsi14", np.nan)
        roe = r.get("roe_pct", np.nan)
        der = r.get("der", np.nan)

        # --- Entry 1 ---
        if pd.notna(close) and pd.notna(sup1):
            entry1 = min(float(close), float(sup1) * cfg["M4_E1_SUPPORT_TOL"])
        elif pd.notna(sup1):
            entry1 = float(sup1) * cfg["M4_E1_SUPPORT_TOL"]
        elif pd.notna(close):
            entry1 = float(close)
        else:
            entry1 = np.nan

        # Validasi Entry 1
        if pd.notna(entry1) and pd.notna(sup_maj) and entry1 < sup_maj:
            e1_status = "TIDAK VALID"
        elif (pd.notna(rsi) and rsi < cfg["M4_E1_RSI_MAX"]
              and pd.notna(close) and pd.notna(sup1)
              and close <= sup1 * cfg["M4_E1_SUPPORT_TOL"]):
            e1_status = "AKTIF"
        else:
            e1_status = "MENUNGGU"

        # --- Entry 2 ---
        entry2 = entry1 * (1 - 0.065) if pd.notna(entry1) else np.nan
        dd_from_e1 = (entry1 - close) / entry1 if (pd.notna(entry1) and entry1 > 0 and pd.notna(close)) else np.nan
        e2_price_ok = (pd.notna(dd_from_e1)
                       and cfg["M4_E2_DD_MIN"] <= dd_from_e1 <= cfg["M4_E2_DD_MAX"])
        e2_support_ok = (pd.notna(sup_maj) and pd.notna(close) and close > sup_maj)
        if pd.notna(entry2) and pd.notna(sup_maj) and entry2 < sup_maj:
            e2_status = "TIDAK VALID"
        elif e2_price_ok and e2_support_ok:
            e2_status = "AKTIF"
        else:
            e2_status = "MENUNGGU"

        # --- Entry 3 ---
        entry3 = entry2 * (1 - 0.10) if pd.notna(entry2) else np.nan
        dd_from_e2 = (entry2 - close) / entry2 if (pd.notna(entry2) and entry2 > 0 and pd.notna(close)) else np.nan
        e3_price_ok = (pd.notna(dd_from_e2)
                       and cfg["M4_E3_DD_MIN"] <= dd_from_e2 <= cfg["M4_E3_DD_MAX"])
        e3_fund_ok = (pd.notna(roe) and roe > cfg["M4_E3_ROE_MIN"]
                      and pd.notna(der) and der < cfg["M4_E3_DER_MAX"])
        if pd.notna(entry3) and pd.notna(sup_maj) and entry3 < sup_maj:
            e3_status = "TIDAK VALID"
        elif not e3_fund_ok:
            e3_status = "TIDAK VALID"
        elif e3_price_ok:
            e3_status = "AKTIF"
        else:
            e3_status = "MENUNGGU"

        e1a, e2a, e3a = cfg["M4_E1_ALLOC"], cfg["M4_E2_ALLOC"], cfg["M4_E3_ALLOC"]
        levels = [(entry1, e1a), (entry2, e2a), (entry3, e3a)]
        wsum = sum(w for lv, w in levels if pd.notna(lv))
        avg = (sum(lv * w for lv, w in levels if pd.notna(lv)) / wsum) if wsum > 0 else np.nan

        std_e1 = (f"min(close, support1*{cfg['M4_E1_SUPPORT_TOL']:.2f})"
                  f" & >= support_major → {e1_status}")
        std_e2 = (f"turun {cfg['M4_E2_DD_MIN']*100:.0f}-{cfg['M4_E2_DD_MAX']*100:.0f}% dari Entry1"
                  f" & di atas support_major → {e2_status}")
        std_e3 = (f"turun {cfg['M4_E3_DD_MIN']*100:.0f}-{cfg['M4_E3_DD_MAX']*100:.0f}% dari Entry2"
                  f" & ROE>{cfg['M4_E3_ROE_MIN']:.0f} & DER<{cfg['M4_E3_DER_MAX']:.1f}"
                  f" & >= support_major → {e3_status}")

        rows.append({"Nama Perusahaan": nm, "Kode Saham": tk, "Parameter": "Entry 1",
                     "Value": f"{entry1:.0f}" if pd.notna(entry1) else "N/A",
                     "Kriteria": f"alokasi {e1a*100:.0f}%",
                     "Standard yang Dipakai": std_e1})
        rows.append({"Nama Perusahaan": nm, "Kode Saham": tk, "Parameter": "Entry 2",
                     "Value": f"{entry2:.0f}" if pd.notna(entry2) else "N/A",
                     "Kriteria": f"alokasi {e2a*100:.0f}%",
                     "Standard yang Dipakai": std_e2})
        rows.append({"Nama Perusahaan": nm, "Kode Saham": tk, "Parameter": "Entry 3",
                     "Value": f"{entry3:.0f}" if pd.notna(entry3) else "N/A",
                     "Kriteria": f"alokasi {e3a*100:.0f}%",
                     "Standard yang Dipakai": std_e3})
        rows.append({"Nama Perusahaan": nm, "Kode Saham": tk, "Parameter": "Harga rata-rata (40/30/30)",
                     "Value": f"{avg:.0f}" if pd.notna(avg) else "N/A",
                     "Kriteria": "simulasi",
                     "Standard yang Dipakai": "bila ketiga entry terisi"})

    out_df = pd.DataFrame(rows) if rows else _empty_table()
    log(f"diproses -> {n_in} saham, lolos -> {len(passed3)}")
    if len(out_df) > 0:
        with pd.option_context("display.max_rows", None, "display.max_columns", None,
                               "display.width", None, "display.max_colwidth", None):
            print(out_df.to_string(index=False))
    return out_df


# =============================================================
# MODUL 5 - RESUME
# =============================================================
def modul_5(passed1, passed3, cfg):
    """Gabungkan passed1 (kualitas) dengan support1/support_major dari passed3."""
    log("\n" + "=" * 70)
    log("MODUL 5 - RESUME (7 KRITERIA)")
    log("=" * 70)
    n_in = len(passed1)
    if n_in == 0:
        log("diproses -> 0 saham, lolos -> 0")
        return _empty_table(), 0

    support_map = {}
    if passed3 is not None and len(passed3) > 0 and "support1" in passed3.columns:
        for _, r3 in passed3.iterrows():
            support_map[r3["ticker"]] = {
                "support1": r3.get("support1", np.nan),
                "support_major": r3.get("support_major", np.nan),
            }

    scored_list = []
    full_count = 0
    for _, r in passed1.iterrows():
        tk = r["ticker"]
        close = r.get("close", np.nan)
        rsi = r.get("rsi14", np.nan)
        dd = r.get("drawdown_52w", np.nan)
        roe = r.get("roe_pct", np.nan)
        der = r.get("der", np.nan)
        div = r.get("div_yield_pct", np.nan)
        val = r.get("avg_value_20d", np.nan)
        ma50 = r.get("ma50", np.nan)
        ma100 = r.get("ma100", np.nan)
        sup_info = support_map.get(tk, {})
        sup1 = sup_info.get("support1", np.nan)
        sup_maj = sup_info.get("support_major", np.nan)

        near_sup = False
        near_source = "-"
        if pd.notna(close):
            for lv, src in [(ma50, "MA50"), (ma100, "MA100"),
                            (sup1, "support1"), (sup_maj, "support_major")]:
                if pd.notna(lv) and lv > 0:
                    ratio = close / lv
                    if 1.00 <= ratio <= 1.03:
                        near_sup = True
                        near_source = f"{src} ({lv:.0f})"
                        break

        c1 = pd.notna(roe) and roe > cfg["M5_ROE_MIN"]
        c2 = pd.notna(der) and der < cfg["M5_DER_MAX"]
        c3 = pd.notna(div) and div > cfg["M5_DIV_MIN"]
        c4 = pd.notna(rsi) and rsi < cfg["M5_RSI_MAX"]
        c5 = near_sup
        c6 = pd.notna(dd) and cfg["M5_DD_MIN"] <= dd <= cfg["M5_DD_MAX"]
        c7 = pd.notna(val) and val > cfg["M5_VALUE_MIN_M"] * 1e9

        score = int(sum([c1, c2, c3, c4, c5, c6, c7]))
        if score == 7:
            full_count += 1

        scored_list.append({
            "row": r, "score": score,
            "c1": c1, "c2": c2, "c3": c3, "c4": c4, "c5": c5, "c6": c6, "c7": c7,
            "close": close, "rsi": rsi, "dd": dd, "roe": roe, "der": der,
            "div": div, "val": val, "near_source": near_source,
        })

    scored_list.sort(key=lambda x: x["score"], reverse=True)

    rows = []
    for item in scored_list:
        r = item["row"]
        tk = r["ticker"]
        nm = r["nama"] if pd.notna(r["nama"]) else tk
        score = item["score"]
        label = "LOLOS PENUH" if score == 7 else f"{score}/7"

        rows.append({"Nama Perusahaan": nm, "Kode Saham": tk, "Parameter": "ROE",
                     "Value": f"{item['roe']:.2f}%" if pd.notna(item['roe']) else "N/A",
                     "Kriteria": f"> {cfg['M5_ROE_MIN']}% {'✔' if item['c1'] else '✘'}",
                     "Standard yang Dipakai": f"ideal > {cfg['M5_ROE_MIN']}%"})
        rows.append({"Nama Perusahaan": nm, "Kode Saham": tk, "Parameter": "DER",
                     "Value": f"{item['der']:.3f}" if pd.notna(item['der']) else "N/A",
                     "Kriteria": f"< {cfg['M5_DER_MAX']} {'✔' if item['c2'] else '✘'}",
                     "Standard yang Dipakai": f"ideal < {cfg['M5_DER_MAX']}"})
        rows.append({"Nama Perusahaan": nm, "Kode Saham": tk, "Parameter": "Dividend Yield",
                     "Value": f"{item['div']:.2f}%" if pd.notna(item['div']) else "N/A",
                     "Kriteria": f"> {cfg['M5_DIV_MIN']}% {'✔' if item['c3'] else '✘'}",
                     "Standard yang Dipakai": f"> {cfg['M5_DIV_MIN']}%"})
        rows.append({"Nama Perusahaan": nm, "Kode Saham": tk, "Parameter": "RSI14",
                     "Value": f"{item['rsi']:.2f}" if pd.notna(item['rsi']) else "N/A",
                     "Kriteria": f"< {cfg['M5_RSI_MAX']} {'✔' if item['c4'] else '✘'}",
                     "Standard yang Dipakai": f"oversold < {cfg['M5_RSI_MAX']}"})
        rows.append({"Nama Perusahaan": nm, "Kode Saham": tk, "Parameter": "Dekat Support",
                     "Value": f"close {item['close']:.0f}" if pd.notna(item['close']) else "N/A",
                     "Kriteria": f"{'✔' if item['c5'] else '✘'} ({item['near_source']})",
                     "Standard yang Dipakai": "1.00 <= close/level <= 1.03 (MA50/MA100/support1/support_major)"})
        rows.append({"Nama Perusahaan": nm, "Kode Saham": tk, "Parameter": "Drawdown 52w",
                     "Value": f"{item['dd']:.2f}%" if pd.notna(item['dd']) else "N/A",
                     "Kriteria": f"{cfg['M5_DD_MIN']}% - {cfg['M5_DD_MAX']}% {'✔' if item['c6'] else '✘'}",
                     "Standard yang Dipakai": "koreksi wajar"})
        rows.append({"Nama Perusahaan": nm, "Kode Saham": tk, "Parameter": "Likuiditas Harian",
                     "Value": f"Rp {item['val']/1e9:.2f} Miliar/hari" if pd.notna(item['val']) else "N/A",
                     "Kriteria": f"> Rp {cfg['M5_VALUE_MIN_M']} M {'✔' if item['c7'] else '✘'}",
                     "Standard yang Dipakai": f"> Rp {cfg['M5_VALUE_MIN_M']} Miliar"})
        rows.append({"Nama Perusahaan": nm, "Kode Saham": tk, "Parameter": "Skor",
                     "Value": label,
                     "Kriteria": f"{score}/7",
                     "Standard yang Dipakai": "LOLOS PENUH bila 7/7"})

    out_df = pd.DataFrame(rows) if rows else _empty_table()
    log(f"diproses -> {n_in} saham, lolos -> {len(passed1)} (LOLOS PENUH: {full_count})")
    if len(out_df) > 0:
        with pd.option_context("display.max_rows", None, "display.max_columns", None,
                               "display.width", None, "display.max_colwidth", None):
            print(out_df.to_string(index=False))
    return out_df, full_count


# =============================================================
# SAVE
# =============================================================
def save_outputs(master, out1, out2, out3, out4, out5, cfg):
    ensure_dir(cfg["OUTPUT_DIR"])
    pref = os.path.join(cfg["OUTPUT_DIR"], cfg["OUTPUT_PREFIX"])

    # Pastikan tidak ada None
    outputs = {
        "master": master if master is not None else pd.DataFrame(),
        "modul1": out1 if out1 is not None else _empty_table(),
        "modul2": out2 if out2 is not None else _empty_table(),
        "modul3": out3 if out3 is not None else _empty_table(),
        "modul4": out4 if out4 is not None else _empty_table(),
        "modul5": out5 if out5 is not None else _empty_table(),
    }

    try:
        outputs["master"].to_csv(pref + " - master.csv", index=False)
        outputs["modul1"].to_csv(pref + " - modul1.csv", index=False)
        outputs["modul2"].to_csv(pref + " - modul2.csv", index=False)
        outputs["modul3"].to_csv(pref + " - modul3.csv", index=False)
        outputs["modul4"].to_csv(pref + " - modul4.csv", index=False)
        outputs["modul5"].to_csv(pref + " - modul5.csv", index=False)
    except Exception as e:
        log(f"[SIMPAN] gagal menyimpan CSV: {e}")

    try:
        xlsx = pref + ".xlsx"
        with pd.ExcelWriter(xlsx, engine="openpyxl") as w:
            outputs["master"].to_excel(w, sheet_name="master", index=False)
            outputs["modul1"].to_excel(w, sheet_name="modul1_kualitas", index=False)
            outputs["modul2"].to_excel(w, sheet_name="modul2_oversold", index=False)
            outputs["modul3"].to_excel(w, sheet_name="modul3_support", index=False)
            outputs["modul4"].to_excel(w, sheet_name="modul4_averaging", index=False)
            outputs["modul5"].to_excel(w, sheet_name="modul5_resume", index=False)
        log(f"[SIMPAN] Excel multi-sheet: {xlsx}")
    except Exception as e:
        log(f"[SIMPAN] gagal menyimpan Excel: {e}")

    log(f"[SIMPAN] CSV master: {pref} - master.csv")


# =============================================================
# MAIN
# =============================================================
def main():
    cfg = CONFIG
    log("=" * 70)
    log("SCREENING: SAHAM MURAH TAPI BELUM RUSAK (rev6-final)")
    log("=" * 70)

    log(f"\n[1] Membaca ticker dari: {cfg['TICKER_FILE']}")
    try:
        tickers = load_tickers(cfg["TICKER_FILE"])
        log("    sumber: file eksternal")
    except Exception as e:
        log(f"    file tidak ditemukan / error: {e}")
        log("    memakai EMBEDDED_TICKERS (built-in)")
        tickers = list(dict.fromkeys(EMBEDDED_TICKERS))
    log(f"    total ticker unik: {len(tickers)}")
    if len(tickers) == 0:
        log("FATAL: tidak ada ticker untuk diproses.")
        return

    log(f"\n[2] Download harga (period={cfg['PERIOD']})")
    try:
        price_data = download_prices(tickers, cfg)
    except Exception as e:
        log(f"FATAL: download harga gagal: {e}")
        traceback.print_exc()
        return
    if not price_data:
        log("FATAL: tidak ada data harga yang valid.")
        return

    log("\n[3] Download fundamental")
    try:
        fund_df = download_fundamentals(list(price_data.keys()), cfg)
    except Exception as e:
        log(f"[FUND] gagal total, lanjut tanpa fundamental: {e}")
        traceback.print_exc()
        fund_df = pd.DataFrame(columns=["ticker", "nama", "roe_pct", "der",
                                        "growth_yoy_pct", "div_yield_pct",
                                        "market_cap_t", "growth_source"])

    log("\n[4] Menggabungkan data master")
    try:
        master = build_master(price_data, fund_df, cfg)
        log(f"    master shape: {master.shape}")
    except Exception as e:
        log(f"FATAL: build_master gagal: {e}")
        traceback.print_exc()
        return

    log("\n[5] Menjalankan modul screening")
    passed1, out1 = modul_1(master, cfg)
    passed2, out2 = modul_2(passed1, cfg)
    passed3, out3 = modul_3(passed1, price_data, cfg)
    out4 = modul_4(passed3, cfg)
    out5, full_count = modul_5(passed1, passed3, cfg)

    log("\n[6] Menyimpan hasil")
    save_outputs(master, out1, out2, out3, out4, out5, cfg)

    log("\n" + "=" * 70)
    log("RINGKASAN FUNNEL")
    log("=" * 70)
    log(f"Total ticker        : {len(tickers)}")
    log(f"Data valid          : {len(price_data)}")
    log(f"Lolos Modul 1       : {len(passed1)}")
    log(f"Lolos Modul 2       : {len(passed2)}")
    log(f"Lolos Modul 3       : {len(passed3)}")
    log(f"LOLOS PENUH (7/7)   : {full_count}")
    log("\nHasil screening bukan rekomendasi investasi.")


if __name__ == "__main__":
    main()


# =============================================================
# ASUMSI YANG DIPAKAI (rev6-final)
# =============================================================
"""
ASUMSI YANG DIPAKAI (rev6-final):
- RSI14 dihitung dengan metode Wilder (ewm alpha = 1/14).
- DER dari yfinance (debtToEquity) dibagi 100 karena satuan asli dalam persen.
- Net Profit Growth YoY dihitung dari income statement; jika hasil NaN,
  fallback ke info["earningsGrowth"]. Sumber dicatat di growth_source dan
  diteruskan ke master.
- Dividend Yield = total dividen 365 hari terakhir (cutoff absolut dari
  pd.Timestamp.now()) / harga close terakhir × 100. Yield > 12% diberi warning.
  last_close diambil dari history 5d → fast_info (via getattr) → info.regularMarketPrice.
- Cache fundamental di-reuse jika umur <= FUND_CACHE_MAX_AGE_DAYS (7 hari).
- "Dekat support" di Modul 3 & Modul 5: 1.00 <= close/level <= 1.03 untuk
  MA50/MA100/support1/support_major; atau close di dalam zona Fibonacci/Demand.
- Support Major: horizontal kuat tepat di bawah support1, maks 25% lebih rendah;
  fallback ke low 60 hari.
- Alokasi averaging 40% / 30% / 30%.
- Entry 1 = min(close, support1 × 1.02). Entry 2 = Entry1 × 0.935.
  Entry 3 = Entry2 × 0.90.
- Entry yang jatuh di bawah support_major → status "TIDAK VALID".
- Filter likuiditas harian opsional (M1_FILTER_LIQUIDITY, default True) dengan
  ambang M1_VALUE_MIN_M = Rp 10 Miliar/hari, dijalankan di Modul 1.
- Data fundamental kosong = NaN dan dianggap TIDAK lolos. Diagnostik M1
  memisahkan NaN vs di luar batas untuk setiap kriteria.
- Screening ini bukan rekomendasi investasi.
"""