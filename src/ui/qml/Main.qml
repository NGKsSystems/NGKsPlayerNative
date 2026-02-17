import QtQuick
import QtQuick.Window

Window {
    id: root
    width: 900
    height: 520
    visible: true
    title: "NGKsPlayerNative - Phase C Qt Quick Boot"
    color: "#131313"

    property real meterL: 0.25
    property real meterR: 0.55

    Timer {
        interval: 120
        running: true
        repeat: true
        onTriggered: {
            meterL = 0.12 + Math.random() * 0.84
            meterR = 0.12 + Math.random() * 0.84
        }
    }

    Column {
        anchors.centerIn: parent
        spacing: 18

        Text {
            text: "NGKsPlayerNative — Qt Quick Window"
            color: "#f1f1f1"
            font.pixelSize: 30
            horizontalAlignment: Text.AlignHCenter
            width: 520
        }

        Text {
            text: "Phase C: QML rendering check"
            color: "#bdbdbd"
            font.pixelSize: 16
            horizontalAlignment: Text.AlignHCenter
            width: 520
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
                        height: parent.height * meterL
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
                        height: parent.height * meterR
                        radius: 8
                        color: "#2fa0ff"
                    }
                }
            }
        }
    }
}