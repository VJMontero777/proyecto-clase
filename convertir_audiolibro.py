import os
import re
import time
import random
import asyncio
import subprocess
from pathlib import Path
from typing import List

from pypdf import PdfReader
import edge_tts

# ========== CONFIGURACIÓN ==========
PDF_FILENAME = "El_Principito.pdf"   # nombre exacto del PDF
OUTPUT_MP3   = "El_Principito.mp3"   # archivo final
TMP_DIR      = Path("tmp_audio_parts")

# Voces masculinas (elige UNA):
#   "es-MX-JorgeNeural"  -> latino neutro
#   "es-ES-AlvaroNeural" -> más grave (España)
#   "es-PE-AlexNeural"
#   "es-AR-TomasNeural"
#   "es-CO-GonzaloNeural"
VOICE        = "es-MX-JorgeNeural"   # cámbiala si prefieres Álvaro

# Ritmo (sin PITCH para evitar 'Invalid pitch')
RATE         = "-10%"   # entre -12% y -6% suena más locutor

# Robustez (límite del servicio/red)
CHUNK_SIZE   = 1000     # más pequeño => menos fallos
RETRIES      = 10       # intentos por parte
BASE_SLEEP   = 5        # backoff exponencial base (s)
PAUSE_BETWEEN_PARTS = 6.0  # pausa fija entre partes (s)
# ===================================


def leer_pdf_completo(pdf_path: Path) -> str:
    reader = PdfReader(str(pdf_path))
    textos = []
    for page in reader.pages:
        try:
            t = page.extract_text() or ""
        except Exception:
            t = ""
        if t.strip():
            textos.append(t)
    return "\n\n".join(textos)


def limpiar_texto(texto: str) -> str:
    texto = texto.replace("\r", "\n")
    texto = re.sub(r"[ \t]+", " ", texto)
    texto = re.sub(r"\n{3,}", "\n\n", texto)
    texto = re.sub(r"-\n", "", texto)         # une palabras cortadas
    texto = re.sub(r"\s+\n", "\n", texto)
    texto = re.sub(r"([\.!\?])([^\s])", r"\1 \2", texto)  # espacio tras . ! ?
    return texto.strip()


def partir_en_chunks(texto: str, max_len: int) -> List[str]:
    if len(texto) <= max_len:
        return [texto]
    partes: List[str] = []
    actual: List[str] = []
    parrafos = texto.split("\n\n")
    for p in parrafos:
        p = p.strip()
        if not p:
            continue
        if len(p) > max_len:
            oraciones = re.split(r'(?<=[\.\?\!])\s+', p)
            bloque = ""
            for o in oraciones:
                if len(bloque) + len(o) + 1 <= max_len:
                    bloque += (o + " ")
                else:
                    if bloque.strip():
                        partes.append(bloque.strip())
                    while len(o) > max_len:
                        partes.append(o[:max_len])
                        o = o[max_len:]
                    bloque = o + " "
            if bloque.strip():
                partes.append(bloque.strip())
        else:
            if actual and (len(" ".join(actual)) + len(p) + 2) > max_len:
                partes.append(" ".join(actual).strip())
                actual = [p]
            else:
                actual.append(p)
    if actual:
        partes.append(" ".join(actual).strip())
    return [x for x in partes if x.strip()]


async def _edge_tts_save_text(texto: str, salida: Path):
    # Modo TEXTO (sin SSML y sin 'pitch' param)
    communicate = edge_tts.Communicate(texto, voice=VOICE, rate=RATE)
    await communicate.save(str(salida))


def generar_parte(texto: str, ruta_mp3: Path):
    last_err = None
    for intento in range(1, RETRIES + 1):
        try:
            # pequeño jitter previo
            time.sleep(random.uniform(0.4, 1.2))
            asyncio.run(_edge_tts_save_text(texto, ruta_mp3))
            return
        except Exception as e:
            last_err = e
            sleep_s = BASE_SLEEP * (2 ** (intento - 1)) + random.uniform(0, 2.0)
            print(f"   ⚠️  Fallo TTS (intento {intento}/{RETRIES}). Reintentando en {sleep_s:.1f}s...")
            time.sleep(sleep_s)
    raise RuntimeError(f"No se pudo generar {ruta_mp3.name}: {last_err}")


def unir_con_ffmpeg(mp3_parts: List[Path], salida: Path):
    list_file = salida.parent / "concat_list.txt"
    with open(list_file, "w", encoding="utf-8") as f:
        for p in mp3_parts:
            f.write(f"file '{p.as_posix()}'\n")
    cmd = ["ffmpeg","-y","-f","concat","-safe","0","-i",str(list_file),"-c","copy",str(salida)]
    subprocess.run(cmd, check=True)
    list_file.unlink(missing_ok=True)


def main():
    # Fix event loop Windows
    if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        except Exception:
            pass

    base = Path(__file__).parent.resolve()
    pdf_path = base / PDF_FILENAME
    out_final = base / OUTPUT_MP3
    if not pdf_path.exists():
        raise FileNotFoundError(f"No se encontró el PDF: {pdf_path}")

    print("Leyendo PDF...")
    texto = leer_pdf_completo(pdf_path)

    print("Limpiando texto...")
    texto = limpiar_texto(texto)
    if not texto.strip():
        raise ValueError("No se pudo extraer texto del PDF (puede ser un PDF de solo imágenes).")

    print("Partiendo en fragmentos...")
    partes = partir_en_chunks(texto, CHUNK_SIZE)
    total = len(partes)
    print(f"Total de fragmentos: {total}")

    TMP_DIR.mkdir(exist_ok=True)
    mp3_parts: List[Path] = []

    for i, contenido in enumerate(partes, start=1):
        parte_path = TMP_DIR / f"parte_{i:03d}.mp3"
        if parte_path.exists() and parte_path.stat().st_size > 0:
            print(f"Saltando parte {i}/{total} (ya existe)...")
            mp3_parts.append(parte_path)
            continue

        print(f"Generando parte {i}/{total}...")
        generar_parte(contenido, parte_path)
        mp3_parts.append(parte_path)

        # Pausa fija entre partes para evitar rate-limit
        time.sleep(PAUSE_BETWEEN_PARTS)

    print("Uniendo con FFmpeg (sin recodificar)...")
    mp3_parts = sorted(set(mp3_parts), key=lambda p: p.name)
    unir_con_ffmpeg(mp3_parts, out_final)

    print(f"\n✅ Listo: {out_final.name}")
    print("Reprodúcelo con VLC o Windows Media Player, o pásalo a tu celular.")


if __name__ == "__main__":
    main()
