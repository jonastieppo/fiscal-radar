    //Functions to get the content of the selected enterprise, and send it to the flask server, which will save in the system
    const flaskUrl = 'http://127.0.0.1:5000/'

    async function getAllCnpj(){
        const response = await fetch(flaskUrl+'cnpj', {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
            },
        });

        return JSON.parse(await response.text())

    }

function populateCnpj(){
        getAllCnpj().then((data)=>{
            const counter = +localStorage.getItem('counter')
            cnpjEntry = document.getElementById("cnpj")
            console.log(`Preenchendo cnpj ${data[counter]}`)
            cnpjEntry.value = formatCNPJ(data[counter])
            localStorage.setItem('cnpj', data[counter])
            localStorage.setItem('counter', counter+1)

            fetch(flaskUrl, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
            },
        })
        }).then(()=>{
            console.log('Check if not robot was clicked!')
        })
    }

    function formatCNPJ(numberString) {
            // Remove any non-digit characters from the input
            const cleanNumber = numberString.replace(/\D/g, '');

            // Check if the cleaned number has exactly 14 digits
            if (cleanNumber.length !== 14) {
                return 'Invalid input: Please enter exactly 14 digits.';
            }

            // Apply the CNPJ mask: XX.XXX.XXX/XXXX-XX
            // Substring(start, length)
            const part1 = cleanNumber.substring(0, 2);
            const part2 = cleanNumber.substring(2, 5);
            const part3 = cleanNumber.substring(5, 8);
            const part4 = cleanNumber.substring(8, 12);
            const part5 = cleanNumber.substring(12, 14);

            return `${part1}.${part2}.${part3}/${part4}-${part5}`;
        }

populateCnpj()
