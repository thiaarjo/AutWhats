@echo off
title AutWhats - Robo Automatico
echo ========================================================
echo Iniciando o Robo AutWhats...
echo Lendo configuracoes do arquivo config.json...
echo ========================================================

call .venv\Scripts\activate
python whatsapp_listener.py

echo.
echo Sessao finalizada.
pause
