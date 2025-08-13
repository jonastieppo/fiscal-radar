#!/bin/sh
# Paste javascript code and bypass recaptcha
xdotool mousemove 515 524 click 1 #move o mouse para a janela
xdotool key "ctrl+l" #clear console

# ----------------------------------------------------------------- #
#               Colando código em javacript no console		    #
# ----------------------------------------------------------------  #

# - Populando CNPJ
# xdotool type "$(cat cnpjSetter.js)"


# - Cliando  para cima 4x
echo ....arrow up...
xdotool key "Up"
echo ....arrow up...

xdotool key "Up"
xdotool key "Return" "Return"

sleep 1 #wait 1 second (i can break if it fails)
echo populating fields...
xdotool type "populateCnpj()"
sleep 2 #wait 1 second (i can break if it fails)
xdotool key "Return" "Return"


sleep 2 #wait 4 second (i can break if it fails)
xdotool mousemove 515 524 click 1 #move o mouse para a janela
xdotool key "ctrl+l" #clear console
xdotool type "clickConsultar()"
xdotool key "Return" "Return"

sleep 4 #wait two second
xdotool mousemove 515 524 click 1 #move o mouse para a janela
xdotool key "ctrl+l" #clear console
echo .....arrow up...
xdotool key "Up"
echo ....arrow up...
xdotool key "Up"
echo ....arrow up...
xdotool key "Up"
xdotool key "Return" "Return"

xdotool type "getAndSendPDFContent()"
sleep 2 # wait to check the command
xdotool key "Return" "Return"
xdotool mousemove 515 524 click 1 #move o mouse para a janela
xdotool key "ctrl+l" #clear console
sleep 1 # wait 8 seconds to the page return to main url