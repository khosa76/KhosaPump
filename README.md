# KhosaPump - Smart Irrigation Dashboard

Web application for the ESP32 & SIMCom A7672S 4G LTE Agricultural Pump Controller.

## 🌐 Live Pages
- **Punjabi (Default)**: [index.html](index.html)
- **English**: [english.html](english.html)

## ⚡ Features
- **Real-Time Telemetry**: Voltage, Current, 4G LTE CSQ, Carrier, and Mains status.
- **Motor Control**: START / STOP with dual-relay starter sequence.
- **Safety Toggles**:
  - **Auto-Restart on Power**: Automatically resumes motor after grid power outage.
  - **Dry-Run Auto Trip**: User-configurable between **Alert Only** (default) and **Auto-Trip**.
- **Public & Private Broker Support**: Connects to HiveMQ via Secure WebSockets (WSS).
