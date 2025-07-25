    //Functions to get the content of the selected enterprise, and send it to the flask server, which will save in the system

    // Local Storage: Parei no contador 105
    
    const flaskUrl = 'http://127.0.0.1:5000/'

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

    function getAndSendPDFContent() {
        const cnpj = localStorage.getItem('cnpj')
        const content = document.getElementsByClassName("conteudo")[0]
        sendContentToFlask(content, cnpj).then(()=>{
            console.log("Conteúdo enviado!")
            window.location.href = 'https://solucoes.receita.fazenda.gov.br/servicos/cnpjreva/Cnpjreva_Solicitacao.asp'
        })
        .catch(()=> console.log('Deu algum problema'))

    }

getAndSendPDFContent()
