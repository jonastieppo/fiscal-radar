function clickCaptcha(){
    fetch('http://127.0.0.1:5000')
}

function sendCnpjKey(cnpj){
    document.getElementById("cnpj").value = cnpj;
}

function clickSearch(){
    document.getElementsByClassName("btn btn-primary")[0].click()
}

async function sendContentToFlask(content, cnpj){

    const response = await fetch(flaskUrl, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            id: cnpj,
            htmlContent: content.innerText
        }),
    });

}

function getAndSendPDFContent(cnpj) {
    const content = document.getElementsByClassName("conteudo")[0]
    console.log(content)
    sendContentToFlask(content, cnpj).then(()=>{
        console.log("Conteúdo enviado!")
    })
    .catch(()=> console.log('Deu algum problema'))

}
const flaskUrl = "http://127.0.0.1:5000"
clickCaptcha()
sendCnpjKey("28887169000124")
clickSearch()
getAndSendPDFContent(28887169000124)

