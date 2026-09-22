// AgriPump 4G LTE - MQTT Web Dashboard Client
// Connects to HiveMQ via Secure WebSockets (WSS)

(function () {
    // Config defaults (Points to HiveMQ public broker matching ESP32 firmware)
    const DEFAULT_CONFIG = {
        host: "broker.hivemq.com",
        port: 8884,
        path: "/mqtt",
        user: "",
        pass: "",
        topicPrefix: "farm/pump"
    };

    let client = null;
    let config = loadConfig();

    // In-flight command tracking
    let inFlightCmd = null;
    let inFlightStartTime = 0;
    let inFlightTimer = null;
    let targetDeviceId = null;
    const isPunjabi = (document.documentElement.lang === "pa");

    const t = {
        connecting: isPunjabi ? "ਕਨੈਕਟ ਹੋ ਰਿਹਾ ਹੈ..." : "Connecting...",
        connected: isPunjabi ? "ਕਨੈਕਟਡ" : "Connected",
        reconnecting: isPunjabi ? "ਦੁਬਾਰਾ ਕਨੈਕਟ ਹੋ ਰਿਹਾ..." : "Reconnecting...",
        offline: isPunjabi ? "ਔਫਲਾਈਨ" : "Offline",
        error: isPunjabi ? "ਗਲਤੀ" : "Error",
        configError: isPunjabi ? "ਕੌਂਫਿਗ ਗਲਤੀ" : "Config Error",
        espOnline: isPunjabi ? "ESP32 ਆਨਲਾਈਨ" : "ESP32 Online",
        espOffline: isPunjabi ? "ESP32 ਔਫਲਾਈਨ" : "ESP32 Offline",
        motorRunning: isPunjabi ? "ਮੋਟਰ ਚਾਲੂ ਹੈ" : "MOTOR RUNNING",
        motorOff: isPunjabi ? "ਮੋਟਰ ਬੰਦ ਹੈ" : "MOTOR OFF",
        startPump: isPunjabi ? "ਪੰਪ ਚਾਲੂ ਕਰੋ" : "START PUMP",
        starting: isPunjabi ? "ਚਾਲੂ ਹੋ ਰਿਹਾ ਹੈ..." : "STARTING...",
        stopPump: isPunjabi ? "ਪੰਪ ਬੰਦ ਕਰੋ" : "STOP PUMP",
        stopping: isPunjabi ? "ਬੰਦ ਹੋ ਰਿਹਾ ਹੈ..." : "STOPPING...",
        enabled: isPunjabi ? "ਚਾਲੂ" : "ENABLED",
        disabled: isPunjabi ? "ਬੰਦ" : "DISABLED",
        autoResumeEnabled: isPunjabi ? "ਚਾਲੂ" : "ENABLED",
        autoResumeDisabled: isPunjabi ? "ਬੰਦ" : "DISABLED",
        dryRunAutoTripOn: isPunjabi ? "ਆਟੋ-ਟ੍ਰਿਪ ਚਾਲੂ" : "AUTO-TRIP ON",
        dryRunAlertOnly: isPunjabi ? "ਸਿਰਫ਼ ਚੇਤਾਵਨੀ" : "ALERT ONLY",
        dryRunStatusTrip: isPunjabi ? "ਆਟੋ-ਟ੍ਰਿਪ ਸਰਗਰਮ (ਮੋਟਰ ਬੰਦ ਹੋਵੇਗੀ)" : "Auto-Trip Active (Motor Stops)",
        dryRunStatusAlert: isPunjabi ? "ਸਿਰਫ਼ ਚੇਤਾਵਨੀ (ਮੋਟਰ ਨਹੀਂ ਰੁਕੇਗੀ)" : "Alert Only (No Trip)",
        autoResumeStatusOn: isPunjabi ? "ਚਾਲੂ" : "Enabled",
        autoResumeStatusOff: isPunjabi ? "ਬੰਦ" : "Disabled",
        normalMains: isPunjabi ? "ਬਿਜਲੀ ਠੀਕ ਹੈ" : "NORMAL MAINS",
        highVoltage: isPunjabi ? "ਵੋਲਟੇਜ ਵੱਧ ਹੈ" : "HIGH VOLTAGE",
        voltageSag: isPunjabi ? "ਵੋਲਟੇਜ ਘੱਟ ਹੈ" : "VOLTAGE SAG",
        powerCut: isPunjabi ? "ਬਿਜਲੀ ਬੰਦ ਹੈ" : "POWER CUT",
        loadNormal: isPunjabi ? "ਲੋਡ ਆਮ ਹੈ" : "LOAD NORMAL",
        dryRunRisk: isPunjabi ? "ਡਰਾਈ-ਰਨ ਖ਼ਤਰਾ" : "DRY-RUN RISK",
        standby: isPunjabi ? "ਸਟੈਂਡਬਾਏ" : "STANDBY",
        pumpingNormally: isPunjabi ? "ਪਾਣੀ ਪੰਪ ਹੋ ਰਿਹਾ ਹੈ" : "Pumping normally",
        lowCurrentWarning: isPunjabi ? "ਘੱਟ ਕਰੰਟ ਚੇਤਾਵਨੀ!" : "Low current warning!",
        motorStopped: isPunjabi ? "ਮੋਟਰ ਬੰਦ ਹੈ" : "Motor Stopped",
        standbySubtitle: isPunjabi ? "ਸਟਾਰਟਰ ਸਟੈਂਡਬਾਏ 'ਤੇ ਹੈ" : "Starter standby",
        runningSubtitle: (v, i) => isPunjabi ? `${v}V (${i}A) 'ਤੇ ਚੱਲ ਰਹੀ ਹੈ` : `Running at ${v}V (${i}A)`,
        cmdSent: (a) => isPunjabi ? `ਕਮਾਂਡ [${a}] ਭੇਜੀ ਗਈ। ESP32 ਦੀ ਪੁਸ਼ਟੀ ਦੀ ਉਡੀਕ ਹੈ...` : `Command [${a}] sent. Waiting for ESP32 hardware confirmation...`,
        noAck: (c) => isPunjabi ? `10 ਸਕਿੰਟਾਂ ਵਿੱਚ [${c}] ਦੀ ਕੋਈ ਪੁਸ਼ਟੀ ਨਹੀਂ ਮਿਲੀ। 4G ਸਿਗਨਲ ਚੈੱਕ ਕਰੋ।` : `No ACK from ESP32 for [${c}] within 10s. Device may be off or 4G data weak.`
    };

    // DOM Elements
    const brokerStatusPill = document.getElementById("brokerStatusPill");
    const brokerStatusText = document.getElementById("brokerStatusText");
    const lastUpdatedText = document.getElementById("lastUpdatedText");

    // Motor State
    const stateOrb = document.getElementById("stateOrb");
    const motorStatusTitle = document.getElementById("motorStatusTitle");
    const motorStatusSubtitle = document.getElementById("motorStatusSubtitle");

    // Control Buttons
    const btnMotorStart = document.getElementById("btnMotorStart");
    const btnMotorStop = document.getElementById("btnMotorStop");

    // Handshake & Status Bars
    const handshakeBar = document.getElementById("handshakeBar");
    const handshakeIcon = document.getElementById("handshakeIcon");
    const handshakeMsg = document.getElementById("handshakeMsg");
    const btnToggleAutoResume = document.getElementById("btnToggleAutoResume");
    const btnToggleDryRun = document.getElementById("btnToggleDryRun");
    const autoResumeStatusText = document.getElementById("autoResumeStatusText");
    const dryRunArmText = document.getElementById("dryRunArmText");

    // Telemetry Elements
    const voltageVal = document.getElementById("voltageVal");
    const voltageFill = document.getElementById("voltageFill");
    const mainsBadge = document.getElementById("mainsBadge");

    const currentVal = document.getElementById("currentVal");
    const currentFill = document.getElementById("currentFill");
    const currentBadge = document.getElementById("currentBadge");
    const loadStateText = document.getElementById("loadStateText");

    const signalVal = document.getElementById("signalVal");
    const signalBars = document.getElementById("signalBars");
    const carrierBadge = document.getElementById("carrierBadge");
    const networkModeText = document.getElementById("networkModeText");
    const apnText = document.getElementById("apnText");

    // Alerts Banner
    const alertBanner = document.getElementById("alertBanner");
    const alertTitle = document.getElementById("alertTitle");
    const alertDesc = document.getElementById("alertDesc");
    const btnDismissAlert = document.getElementById("btnDismissAlert");

    // Log Stream
    const logContainer = document.getElementById("logContainer");
    const btnClearLogs = document.getElementById("btnClearLogs");

    // Settings Modal
    const settingsModal = document.getElementById("settingsModal");
    const btnOpenSettings = document.getElementById("btnOpenSettings");
    const btnCloseSettings = document.getElementById("btnCloseSettings");
    const btnCancelSettings = document.getElementById("btnCancelSettings");
    const settingsForm = document.getElementById("settingsForm");

    const cfgHost = document.getElementById("cfgHost");
    const cfgPort = document.getElementById("cfgPort");
    const cfgPath = document.getElementById("cfgPath");
    const cfgUser = document.getElementById("cfgUser");
    const cfgPass = document.getElementById("cfgPass");
    const cfgTopicPrefix = document.getElementById("cfgTopicPrefix");

    // Initialize
    function init() {
        bindEvents();
        populateSettingsForm();

        // Auto-connect to broker
        if (!config.host) {
            appendLog("info", "Please configure MQTT broker host in Settings.");
            openSettings();
        } else {
            connectMQTT();
        }
    }

    function loadConfig() {
        try {
            const saved = localStorage.getItem("agripump_mqtt_cfg");
            if (saved) {
                const parsed = JSON.parse(saved);
                // If saved host was the old private cluster that didn't match ESP32, reset to default
                if (parsed.host && parsed.host.includes("hivemq.cloud")) {
                    localStorage.removeItem("agripump_mqtt_cfg");
                    return Object.assign({}, DEFAULT_CONFIG);
                }
                return Object.assign({}, DEFAULT_CONFIG, parsed);
            }
        } catch (e) {
            console.error(e);
        }
        return Object.assign({}, DEFAULT_CONFIG);
    }

    function saveConfig(newCfg) {
        config = Object.assign({}, config, newCfg);
        localStorage.setItem("agripump_mqtt_cfg", JSON.stringify(config));
    }

    function populateSettingsForm() {
        cfgHost.value = config.host;
        cfgPort.value = config.port;
        cfgPath.value = config.path;
        cfgUser.value = config.user;
        cfgPass.value = config.pass;
        cfgTopicPrefix.value = config.topicPrefix;
    }

    function openSettings() {
        populateSettingsForm();
        settingsModal.classList.add("active");
    }

    function closeSettings() {
        settingsModal.classList.remove("active");
    }

    function bindEvents() {
        btnOpenSettings.addEventListener("click", openSettings);
        btnCloseSettings.addEventListener("click", closeSettings);
        btnCancelSettings.addEventListener("click", closeSettings);

        settingsForm.addEventListener("submit", (e) => {
            e.preventDefault();
            saveConfig({
                host: cfgHost.value.trim(),
                port: parseInt(cfgPort.value.trim(), 10) || 8884,
                path: cfgPath.value.trim(),
                user: cfgUser.value.trim(),
                pass: cfgPass.value.trim(),
                topicPrefix: cfgTopicPrefix.value.trim()
            });
            closeSettings();
            connectMQTT();
        });

        btnDismissAlert.addEventListener("click", () => {
            alertBanner.style.display = "none";
        });

        btnClearLogs.addEventListener("click", () => {
            logContainer.innerHTML = "";
        });

        // Motor Control Buttons
        btnMotorStart.addEventListener("click", () => {
            if (confirm("Turn ON irrigation pump?")) {
                publishCommand("ON");
            }
        });

        btnMotorStop.addEventListener("click", () => {
            if (confirm("Execute 3-step dual-relay STOP sequence?")) {
                publishCommand("OFF");
            }
        });

        if (btnToggleAutoResume) {
            btnToggleAutoResume.addEventListener("click", () => {
                const isCurrentlyActive = btnToggleAutoResume.classList.contains("active");
                const nextCmd = isCurrentlyActive ? "AUTORESUME_OFF" : "AUTORESUME_ON";
                publishCommand(nextCmd);
            });
        }

        if (btnToggleDryRun) {
            btnToggleDryRun.addEventListener("click", () => {
                const isAutoTripActive = btnToggleDryRun.classList.contains("active");
                const nextCmd = isAutoTripActive ? "DRYRUN_OFF" : "DRYRUN_ON";
                publishCommand(nextCmd);
            });
        }
    }

    // Connect to HiveMQ via WebSockets
    function connectMQTT() {
        if (client) {
            client.end(true);
        }

        updateBrokerStatus("connecting", "Connecting...");

        const brokerUrl = `wss://${config.host}:${config.port}${config.path}`;
        const clientId = `web_dashboard_${Math.random().toString(16).substr(2, 8)}`;

        const options = {
            clientId: clientId,
            keepalive: 60,
            clean: true,
            reconnectPeriod: 4000,
            connectTimeout: 10000
        };
        if (config.user && config.user.trim().length > 0) {
            options.username = config.user;
        }
        if (config.pass && config.pass.trim().length > 0) {
            options.password = config.pass;
        }

        appendLog("info", `Connecting to MQTT Broker: ${config.host}:${config.port}...`);

        try {
            client = mqtt.connect(brokerUrl, options);

            client.on("connect", () => {
                updateBrokerStatus("connected", "Connected");
                appendLog("info", "✓ Connected to MQTT Broker over Secure WebSockets.");

                // Subscribe to telemetry, alerts, status, ACK, and device-specific paths
                const telemetryTopic = `${config.topicPrefix}/telemetry`;
                const alertTopic = `${config.topicPrefix}/alert`;
                const statusTopic = `${config.topicPrefix}/status`;
                const ackTopic = `${config.topicPrefix}/ack`;
                const wildcardTopic = `${config.topicPrefix}/#`;

                client.subscribe([telemetryTopic, alertTopic, statusTopic, ackTopic, wildcardTopic], (err) => {
                    if (err) {
                        appendLog("alert", `Subscription error: ${err.message}`);
                    } else {
                        appendLog("info", `Subscribed to ${config.topicPrefix}/# (Telemetry, Alerts, ACK)`);
                        // Silent probe for device status on connect
                        publishCommand("STATUS", {}, true);
                    }
                });
            });

            client.on("message", (topic, message) => {
                handleIncomingMessage(topic, message.toString());
            });

            client.on("error", (err) => {
                console.error("MQTT Error:", err);
                updateBrokerStatus("error", "Error");
                appendLog("alert", `MQTT error: ${err.message || "Connection failed"}`);
            });

            client.on("offline", () => {
                updateBrokerStatus("error", "Offline");
            });

            client.on("reconnect", () => {
                updateBrokerStatus("connecting", "Reconnecting...");
            });

        } catch (err) {
            console.error("Connection exception:", err);
            updateBrokerStatus("error", "Config Error");
            appendLog("alert", `Connection failed: ${err.message}`);
        }
    }

    function publishCommand(action, extraData = {}, isBackgroundQuery = false) {
        if (!client || !client.connected) {
            alert("Not connected to MQTT broker! Please check your credentials in Settings.");
            openSettings();
            return;
        }

        // Set in-flight tracking
        inFlightCmd = action;
        inFlightStartTime = Date.now();
        if (!isBackgroundQuery) {
            setButtonsLoading(action, true);
        }

        // Update handshake banner to waiting state
        if (handshakeBar && !isBackgroundQuery) {
            handshakeBar.className = "handshake-bar pending";
            handshakeIcon.textContent = "⏳";
            handshakeMsg.textContent = `Command [${action}] sent. Waiting for ESP32 hardware confirmation...`;
        }

        // Arm 10-second timeout for ESP32 response (adapted for cellular 4G latency)
        if (inFlightTimer) clearTimeout(inFlightTimer);
        inFlightTimer = setTimeout(() => {
            if (inFlightCmd) {
                setButtonsLoading(inFlightCmd, false);
                if (!isBackgroundQuery && handshakeBar) {
                    handshakeBar.className = "handshake-bar rejected";
                    handshakeIcon.textContent = "⚠️";
                    handshakeMsg.textContent = `No ACK from ESP32 for [${inFlightCmd}] within 10s. Device may be off or 4G data weak.`;
                    appendLog("alert", `⚠️ Handshake Timeout: ESP32 did not respond to [${inFlightCmd}] within 10s.`);
                }
                // Retain inFlightCmd so if a late response arrives, it can still be credited!
            }
        }, isBackgroundQuery ? 3000 : 10000);

        const cmdTopic = targetDeviceId ? `${config.topicPrefix}/${targetDeviceId}/command` : `${config.topicPrefix}/command`;
        const payload = JSON.stringify(Object.assign({
            action: action,
            sender: "web_dashboard",
            timestamp: new Date().toISOString()
        }, extraData));

        client.publish(cmdTopic, payload, { qos: 1 }, (err) => {
            if (err) {
                clearTimeout(inFlightTimer);
                setButtonsLoading(action, false);
                appendLog("alert", `Failed to publish ${action}: ${err.message}`);
            } else {
                appendLog("cmd", `📤 Sent [${action}] -> ${cmdTopic}`);
            }
        });
    }

    function setButtonsLoading(action, isLoading) {
        const btns = [btnMotorStart, btnMotorStop];
        btns.forEach(b => {
            if (b) b.disabled = isLoading;
        });

        if (action === "ON" && btnMotorStart) {
            const span = btnMotorStart.querySelector(".btn-text");
            if (span) span.textContent = isLoading ? t.starting : t.startPump;
        } else if (action === "OFF" && btnMotorStop) {
            const span = btnMotorStop.querySelector(".btn-text");
            if (span) span.textContent = isLoading ? t.stopping : t.stopPump;
        }
    }

    // Process incoming MQTT payloads
    function handleIncomingMessage(topic, payloadStr) {
        try {
            const data = JSON.parse(payloadStr);

            if (data.deviceId && !targetDeviceId) {
                targetDeviceId = data.deviceId;
                appendLog("info", `🎯 Auto-targeted active pump device: [${targetDeviceId}]`);
            }

            if (topic.endsWith("/ack")) {
                handleAckMessage(data);
            } else if (topic.endsWith("/telemetry")) {
                updateTelemetryUI(data);
                appendLog("telemetry", `📥 Telemetry: V=${data.voltage}V, I=${data.current}A, Motor=${data.motor}`);
            } else if (topic.endsWith("/status")) {
                if (data.status === "online") {
                    updateBrokerStatus("connected", "ESP32 Online");
                    appendLog("info", "🟢 ESP32 is ONLINE via 4G Cat-1.");
                } else if (data.status === "offline") {
                    updateBrokerStatus("error", "ESP32 Offline");
                    appendLog("alert", "🔴 ESP32 went OFFLINE (LWT connection lost).");
                }
            } else if (topic.endsWith("/alert")) {
                showAlertBanner(data.title || "EMERGENCY ALERT", data.message || payloadStr);
                appendLog("alert", `🚨 ${data.title || "ALERT"}: ${data.message || payloadStr}`);
            }
        } catch (e) {
            appendLog("telemetry", `📥 [${topic}]: ${payloadStr}`);
        }
    }

    // Handle 2-way ACK Handshake from ESP32 (instant or delayed)
    function handleAckMessage(data) {
        if (inFlightTimer) {
            clearTimeout(inFlightTimer);
            inFlightTimer = null;
        }

        const latency = inFlightStartTime ? (Date.now() - inFlightStartTime) : 0;
        const isLate = latency > 10000;
        setButtonsLoading(inFlightCmd || data.cmd, false);
        inFlightCmd = null;

        const timeStr = isLate ? `${(latency / 1000).toFixed(1)}s (Delayed)` : `${latency}ms`;

        if (handshakeBar) {
            if (data.status === "SUCCESS") {
                handshakeBar.className = "handshake-bar success";
                handshakeIcon.textContent = "✓";
                handshakeMsg.textContent = `[ACK in ${timeStr}]: ${data.msg || "Command Executed"}`;
                appendLog("cmd", `✅ [ESP32 ACK in ${timeStr}]: ${data.msg}`);
            } else if (data.status === "REJECTED") {
                handshakeBar.className = "handshake-bar rejected";
                handshakeIcon.textContent = "❌";
                handshakeMsg.textContent = `[REJECTED]: ${data.msg || "Command rejected by controller"}`;
                appendLog("alert", `❌ [ESP32 Rejected]: ${data.msg}`);
                showAlertBanner("COMMAND REJECTED", data.msg);
            } else {
                handshakeBar.className = "handshake-bar rejected";
                handshakeIcon.textContent = "⚠️";
                handshakeMsg.textContent = `[FAILED]: ${data.msg || "Execution error"}`;
                appendLog("alert", `⚠️ [ESP32 Error]: ${data.msg}`);
            }
        }
    }

    function updateTelemetryUI(data) {
        const now = new Date();
        lastUpdatedText.textContent = data.time ? `Tower: ${data.time}` : `Updated: ${now.toLocaleTimeString()}`;

        // Voltage
        if (typeof data.voltage === "number") {
            const v = data.voltage;
            voltageVal.textContent = v.toFixed(1);
            const vPct = Math.min(100, Math.max(0, (v / 280) * 100));
            voltageFill.style.width = `${vPct}%`;

            if (v >= 180 && v <= 260) {
                mainsBadge.textContent = t.normalMains;
                mainsBadge.className = "badge badge-green";
                voltageFill.style.background = "linear-gradient(90deg, #10b981 0%, #34d399 100%)";
            } else if (v > 260 && v <= 280) {
                mainsBadge.textContent = t.highVoltage;
                mainsBadge.className = "badge";
                mainsBadge.style.color = "var(--color-amber)";
                voltageFill.style.background = "linear-gradient(90deg, #f59e0b 0%, #fbbf24 100%)";
            } else if (v >= 120 && v < 180) {
                mainsBadge.textContent = t.voltageSag;
                mainsBadge.className = "badge";
                mainsBadge.style.color = "var(--color-amber)";
                voltageFill.style.background = "linear-gradient(90deg, #f59e0b 0%, #fbbf24 100%)";
            } else {
                mainsBadge.textContent = t.powerCut;
                mainsBadge.className = "badge badge-red";
                voltageFill.style.background = "linear-gradient(90deg, #ef4444 0%, #f87171 100%)";
            }
        }

        // Current (Amps)
        if (typeof data.current === "number") {
            const a = data.current;
            currentVal.textContent = a.toFixed(1);
            const aPct = Math.min(100, Math.max(0, (a / 25) * 100));
            currentFill.style.width = `${aPct}%`;

            if (a > 6.0) {
                currentBadge.textContent = t.loadNormal;
                currentBadge.className = "badge badge-green";
                loadStateText.textContent = t.pumpingNormally;
            } else if (data.motor === "ON" && a < 6.0) {
                currentBadge.textContent = t.dryRunRisk;
                currentBadge.className = "badge badge-red";
                loadStateText.textContent = t.lowCurrentWarning;
            } else {
                currentBadge.textContent = t.standby;
                currentBadge.className = "badge";
                loadStateText.textContent = t.motorStopped;
            }
        }

        // Motor State
        if (data.motor) {
            stateOrb.classList.remove("running");
            motorStatusTitle.classList.remove("text-running");

            if (data.motor === "ON") {
                stateOrb.classList.add("running");
                motorStatusTitle.textContent = t.motorRunning;
                motorStatusTitle.classList.add("text-running");
                motorStatusSubtitle.textContent = t.runningSubtitle(data.voltage || 230, data.current || 0);
            } else {
                motorStatusTitle.textContent = t.motorOff;
                motorStatusSubtitle.textContent = t.standbySubtitle;
            }
        }

        // Auto-Resume Toggle state
        if (data.autoResume !== undefined) {
            if (btnToggleAutoResume) {
                if (data.autoResume) {
                    btnToggleAutoResume.className = "toggle-pill active";
                    btnToggleAutoResume.textContent = t.autoResumeEnabled;
                } else {
                    btnToggleAutoResume.className = "toggle-pill";
                    btnToggleAutoResume.textContent = t.autoResumeDisabled;
                }
            }
            if (autoResumeStatusText) {
                autoResumeStatusText.textContent = data.autoResume ? t.autoResumeStatusOn : t.autoResumeStatusOff;
                autoResumeStatusText.className = data.autoResume ? "text-green" : "text-muted";
            }
        }

        // Dry-Run Auto Trip Toggle state
        const dryRunTripActive = (data.dryRunProt !== undefined) ? data.dryRunProt : data.dryRunAutoTrip;
        if (dryRunTripActive !== undefined) {
            if (btnToggleDryRun) {
                if (dryRunTripActive) {
                    btnToggleDryRun.className = "toggle-pill active";
                    btnToggleDryRun.textContent = t.dryRunAutoTripOn;
                } else {
                    btnToggleDryRun.className = "toggle-pill warning-active";
                    btnToggleDryRun.textContent = t.dryRunAlertOnly;
                }
            }
            if (dryRunArmText) {
                dryRunArmText.textContent = dryRunTripActive ? t.dryRunStatusTrip : t.dryRunStatusAlert;
                dryRunArmText.className = dryRunTripActive ? "text-green" : "text-amber";
            }
        }

        // Cellular 4G link
        const sig = (typeof data.signal === "number") ? data.signal : (typeof data.csq === "number" ? data.csq : null);
        if (sig !== null) {
            signalVal.textContent = sig;
            updateSignalBars(sig);
        }
        const carrier = data.carrier || data.operator;
        if (carrier) {
            carrierBadge.textContent = carrier.toUpperCase();
        }
        const mode = data.mode || data.netMode;
        if (mode) {
            networkModeText.textContent = mode;
        }
        const apn = data.apn || (carrier ? (carrier.toLowerCase().includes("airtel") ? "airtelgprs.com" : (carrier.toLowerCase().includes("jio") ? "jionet" : "auto-apn")) : null);
        if (apn) {
            apnText.textContent = apn;
        }
    }

    function updateSignalBars(csq) {
        signalBars.className = "signal-bars";
        if (csq >= 22) signalBars.classList.add("lvl-4");
        else if (csq >= 15) signalBars.classList.add("lvl-3");
        else if (csq >= 8) signalBars.classList.add("lvl-2");
        else if (csq > 0) signalBars.classList.add("lvl-1");
    }

    function showAlertBanner(title, desc) {
        alertTitle.textContent = title;
        alertDesc.textContent = desc;
        alertBanner.style.display = "flex";
    }

    function updateBrokerStatus(state, label) {
        brokerStatusPill.className = `status-pill ${state}`;
        if (label === "Connecting...") label = t.connecting;
        else if (label === "Connected") label = t.connected;
        else if (label === "Reconnecting...") label = t.reconnecting;
        else if (label === "Offline") label = t.offline;
        else if (label === "Error") label = t.error;
        else if (label === "Config Error") label = t.configError;
        else if (label === "ESP32 Online") label = t.espOnline;
        else if (label === "ESP32 Offline") label = t.espOffline;
        brokerStatusText.textContent = label;
    }

    function appendLog(type, text) {
        const time = new Date().toLocaleTimeString();
        const entry = document.createElement("div");
        entry.className = `log-entry log-${type}`;
        entry.innerHTML = `<span class="log-time">${time}</span><span class="log-msg">${escapeHtml(text)}</span>`;
        logContainer.prepend(entry);

        while (logContainer.children.length > 50) {
            logContainer.removeChild(logContainer.lastChild);
        }
    }

    function escapeHtml(str) {
        return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
    }

    window.addEventListener("DOMContentLoaded", init);
})();
