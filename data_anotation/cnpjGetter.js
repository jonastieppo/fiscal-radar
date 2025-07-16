function clickCaptcha(){
    fetch('http://127.0.0.1:5000')
}

function sendCnpjKey(cnpj){
    document.getElementById("cnpj").value = cnpj;
}

function clickSearch(){
    document.getElementsByClassName("btn btn-primary")[0].click()
}

function getPDFContent() {
    const conteudo = document.getElementsByClassName("conteudo")[0]

    // fazer logica para enviar esse conteudo ao flask do python. Ele vai guardar
    // em um documento simples, com ids crescentes. Depois, para cada ID, a data de emissão
    // é retirada.
}

clickCaptcha()
sendCnpjKey("28887169000124")
clickSearch()

function clickAtCoordinates(x, y) {
    const ev = new MouseEvent('click', {
        'view': window,
        'bubbles': true,
        'cancelable': true,
        'screenX': x, // Absolute screen X coordinate
        'screenY': y, // Absolute screen Y coordinate
        'clientX': x, // Relative to the viewport
        'clientY': y  // Relative to the viewport
    });

    // Find the element at the specified coordinates
    const element = document.elementFromPoint(x, y);

    if (element) {
        element.dispatchEvent(ev);
        console.log(`Clicked element at (${x}, ${y}):`, element);
    } else {
        console.log(`No element found at (${x}, ${y}).`);
    }
}

