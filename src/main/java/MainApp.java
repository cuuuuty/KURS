import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class MainApp {
    private static boolean isWorking = true;

    private static Logger LOGGER;
    static {
        try(FileInputStream ins = new FileInputStream("src/main/resources/log.config")){
            LogManager.getLogManager().readConfiguration(ins);
            LOGGER = Logger.getLogger(MainApp.class.getName());
        }catch (Exception e){
            LOGGER.log(Level.WARNING, "Ошибка при иницализации logger", e);
            isWorking = false;
        }
    }

    private static Properties properties = new Properties();
    static {
        try {
            properties.load(new FileInputStream("src/main/resources/app.properties"));
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Ошибка при иницализации properties", e);
            isWorking = false;
        }
    }

    private static final String broker = properties.getProperty("brokerMQTT");
    private static final String userName = properties.getProperty("userNameMQTT");
    private static final String password = properties.getProperty("passwordMQTT");
    private static final List<String> topics = Arrays.asList(properties.getProperty("topicsMQTT").split("\\|"));

    private static int ILLUMINATION = Integer.parseInt(properties.getProperty("defaultIllumination"));
    private static int MAX_ILLUMINATION = Integer.parseInt(properties.getProperty("maxLampsIllumination"));
    private static final String sensorPostfix = "/sensor";
    private static final String setIlluminationPostfix = "/setIllumination";
    private static final String lampPostfix = "/lamp";

    private static MqttClient client;
    private static MqttConnectOptions connOpts = new MqttConnectOptions();

    static {
        MemoryPersistence persistence = new MemoryPersistence();
        try {
            client = new MqttClient(broker, UUID.randomUUID().toString().replace("-", ""), persistence);
            connOpts.setUserName(userName);
            connOpts.setPassword(password.toCharArray());
            connOpts.setCleanSession(true);
            client.setCallback(new MqttCallback() {

                @Override
                public void connectionLost(Throwable throwable) {
                    LOGGER.log(Level.WARNING, "Ошибка при иницализации MqttClient", throwable);
                    isWorking = false;
                }

                @Override
                public void messageArrived(String topicInforming, MqttMessage mqttMessage) {
                    LOGGER.info("Получено сообщение: " + topicInforming + " " + new String(mqttMessage.getPayload()));
                    handleSensorMessage(topicInforming, new String(mqttMessage.getPayload()));
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

                }
            });
        } catch (MqttException e) {
            LOGGER.log(Level.WARNING, "Ошибка при иницализации MqttClient", e);
            isWorking = false;
        }
    }

    public static void main(String[] args) {
        Thread sensorEmulationThread = new SensorEmulationThread();
        Thread lampEmulationThread = new LampEmulationThread();
        sensorEmulationThread.start();
        lampEmulationThread.start();

        try (Scanner scanner = new Scanner(System.in)) {
            while (isWorking) {
                System.out.println("Введите номер необходимого действия:" +
                        "\n1 — Запуск работы модуля" +
                        "\n2 — Установка поддерживаемого уровня освещённости" +
                        "\n3 — Остановка работы модуля(выключение ламп)");

                int currentAction = 0;

                try {
                    currentAction = Integer.parseInt(scanner.nextLine());
                }
                catch(NumberFormatException e){
                    System.out.println("Некорректное действие");
                    continue;
                }

                switch (currentAction) {
                    case 1:
                        startSystem();
                        break;

                    case 2:
                        while (true) {
                            System.out.println("Введите необходимую освещённость (целое число):");
                            int newIllumination = 0;
                            try {
                                newIllumination = Integer.parseInt(scanner.nextLine());
                            } catch (NumberFormatException e) {
                                System.out.println("Некорректное действие");
                                continue;
                            }
                            if (!setRequiredIllumination(newIllumination)) {
                                System.out.println("Освещённость должна быть корректной (больше 0 и меньше maxLampsIllumination=" + MAX_ILLUMINATION + ")");
                            }
                            else
                                break;
                        }
                        break;

                    case 3:
                        stopSystem();
                        break;
                    default:
                        System.out.println("Некорректное действие");
                }
            }
        }
    }

    public static void handleSensorMessage(String topicName, String message) {
        int illuminationFromSensor = Integer.parseInt(message);
        int newLampIllumination;

        if (illuminationFromSensor > ILLUMINATION)
            newLampIllumination = 0;
        else
            newLampIllumination = ILLUMINATION - illuminationFromSensor;

        try {
            String setIlluminationtopic = topicName.substring(0, topicName.length() - sensorPostfix.length()) + setIlluminationPostfix;
            client.publish(setIlluminationtopic, new MqttMessage(String.valueOf(newLampIllumination).getBytes(StandardCharsets.UTF_8)));
//            LOGGER.info("Новый уровень света установлен: " + setIlluminationtopic + " " + newLampIllumination);
        } catch (MqttException e) {
            LOGGER.log(Level.WARNING, "Ошибка при установке освещённости фонаря", e);
            isWorking = false;
        }
    }

    public static void startSystem() {
        try {
            if (!client.isConnected()) {
                client.connect(connOpts);
                for (String topic : topics) {
                    client.subscribe(topic + sensorPostfix);
                }
                System.out.println("Модуль запущен");
                LOGGER.info("Модуль успешно запущен");
            }
            else
                System.out.println("Модуль уже запущен");
        } catch (MqttException e) {
            LOGGER.log(Level.WARNING, "Ошибка при подключении MqttClient", e);
            isWorking = false;
        }
    }

    public static boolean setRequiredIllumination(int illumination) {
        if (illumination <= MAX_ILLUMINATION && illumination >= 0) {
            ILLUMINATION = illumination;
            System.out.println("Поддерживаемая освещённость изменена");
            LOGGER.info("Поддерживаемая освещённость успешно изменена");
            return true;
        }
        else
            return false;
    }

    public static void stopSystem() {
        try {
            if (client.isConnected()) {
                for (String topic : topics) {
                    client.publish(topic + setIlluminationPostfix, new MqttMessage(String.valueOf(0).getBytes(StandardCharsets.UTF_8)));
                }

                client.disconnect();
                System.out.println("Модуль остановлен");
                LOGGER.info("Модуль успешно остановлен");
            }
            else
                System.out.println("Модуль уже остановлен");
        } catch (MqttException e) {
            LOGGER.log(Level.WARNING, "Ошибка при подключении MqttClient", e);
            isWorking = false;
        }
    }

    static class SensorEmulationThread extends Thread {
        @Override
        public void run() {
            MqttClient mqttClientEmulation = null;
            MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
            MemoryPersistence persistence = new MemoryPersistence();
            try {
                mqttClientEmulation = new MqttClient(broker, UUID.randomUUID().toString().replace("-", ""), persistence);
                mqttConnectOptions.setUserName(userName);
                mqttConnectOptions.setPassword(password.toCharArray());
                mqttConnectOptions.setCleanSession(true);
                mqttClientEmulation.connect();
            } catch (MqttException e) {
                LOGGER.log(Level.WARNING, "Ошибка при иницализации MqttClient", e);
                isWorking = false;
                this.interrupt();
            }

            while (!isInterrupted()) {
                try {
                    for (String topic : topics) {
                        int random0_1000 = (int) (Math.random() * 1000);
                        MqttMessage currMessage = new MqttMessage(String.valueOf(random0_1000).getBytes(StandardCharsets.UTF_8));
                        mqttClientEmulation.publish(topic + sensorPostfix, currMessage);
                        Thread.sleep(1000);
                    }
                }
                catch (MqttException | InterruptedException e) {
                    LOGGER.log(Level.WARNING, "Ошибка при эмуляции датчиков", e);
                    isWorking = false;
                }
            }
        }
    }

    static class LampEmulationThread extends Thread {
        private static MqttClient mqttClientLampEmulation = null;

        @Override
        public void run() {
            MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
            MemoryPersistence persistence = new MemoryPersistence();
            try {
                mqttClientLampEmulation = new MqttClient(broker, UUID.randomUUID().toString().replace("-", ""), persistence);
                mqttConnectOptions.setUserName(userName);
                mqttConnectOptions.setPassword(password.toCharArray());
                mqttConnectOptions.setCleanSession(true);
                mqttClientLampEmulation.setCallback(new MqttCallback() {
                    @Override
                    public void connectionLost(Throwable throwable) {
                        LOGGER.log(Level.WARNING, "Ошибка при иницализации MqttClient", throwable);
                        isWorking = false;
                    }

                    @Override
                    public void messageArrived(String topicInforming, MqttMessage mqttMessage) {
                        LOGGER.info("Получено сообщение: " + topicInforming + " " + new String(mqttMessage.getPayload()));

                        if (topicInforming.endsWith(setIlluminationPostfix)) {
                            String topicLamp = topicInforming.substring(0, topicInforming.length() - setIlluminationPostfix.length()) + lampPostfix;
                            // Для эмуляции установки на лампе значения
                            try {
                                mqttClientLampEmulation.publish(topicLamp, new MqttMessage(new String(mqttMessage.getPayload()).getBytes(StandardCharsets.UTF_8)));
                            } catch (MqttException e) {
                                LOGGER.log(Level.WARNING, "Ошибка при установке значения на лампе", e);
                                isWorking = false;
                            }
                        }
                    }

                    @Override
                    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

                    }
                });

                mqttClientLampEmulation.connect();

                for (String topic : topics) {
                    mqttClientLampEmulation.subscribe(topic + setIlluminationPostfix);
                    mqttClientLampEmulation.subscribe(topic + lampPostfix);
                }

            } catch (MqttException e) {
                LOGGER.log(Level.WARNING, "Ошибка при иницализации MqttClient", e);
                isWorking = false;
                this.interrupt();
            }

        }
    }

}


