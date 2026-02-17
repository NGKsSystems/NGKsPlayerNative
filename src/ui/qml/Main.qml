import QtQuick
import QtQuick.Window
import QtQuick.Controls

Window {
    id: root
    width: 900
    height: 520
    visible: true
    title: "NGKsPlayerNative - Phase A"
    color: "#131313"

    Column {
        anchors.centerIn: parent
        spacing: 18

        Text {
            text: "NGKsPlayerNative — Qt + EngineCore"
            color: "#f1f1f1"
            font.pixelSize: 30
            horizontalAlignment: Text.AlignHCenter
            width: 520
        }

        Text {
            text: engine.running ? "Engine running" : "Engine stopped"
            color: "#bdbdbd"
            font.pixelSize: 16
            horizontalAlignment: Text.AlignHCenter
            width: 520
        }

        Row {
            width: 520
            spacing: 12

            Button {
                text: "Play"
                onClicked: engine.start()
            }

            Button {
                text: "Stop"
                onClicked: engine.stop()
            }
        }

        Column {
            width: 520
            spacing: 8

            Text {
                text: "Master Gain (linear): " + gainSlider.value.toFixed(2)
                color: "#d8d8d8"
                font.pixelSize: 14
            }

            Slider {
                id: gainSlider
                width: 520
                from: 0.0
                to: 1.0
                value: 1.0
                onValueChanged: engine.setMasterGain(value)
            }
        }

        Rectangle {
            width: 520
            height: 250
            radius: 10
            color: "#1f1f1f"
            border.color: "#3a3a3a"

            Row {
                anchors.centerIn: parent
                spacing: 90

                Rectangle {
                    width: 96
                    height: 200
                    radius: 8
                    color: "#0f0f0f"
                    border.color: "#444"

                    Rectangle {
                        anchors.left: parent.left
                        anchors.right: parent.right
                        anchors.bottom: parent.bottom
                        height: parent.height * engine.meterL
                        radius: 8
                        color: "#31d17c"
                    }
                }

                Rectangle {
                    width: 96
                    height: 200
                    radius: 8
                    color: "#0f0f0f"
                    border.color: "#444"

                    Rectangle {
                        anchors.left: parent.left
                        anchors.right: parent.right
                        anchors.bottom: parent.bottom
                        height: parent.height * engine.meterR
                        radius: 8
                        color: "#2fa0ff"
                    }
                }
            }
        }
    }
}