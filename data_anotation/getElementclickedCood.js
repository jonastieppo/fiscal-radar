// Metologia para buscar a região em pixel que foi clicada
const elementCLicked = document.getElementById('checkbox');

if (elementCLicked) {
    elementCLicked.addEventListener('click', function(event) {
        const clientX = event.clientX; // X coordinate relative to the viewport
        const clientY = event.clientY; // Y coordinate relative to the viewport

        const screenX = event.screenX; // X coordinate relative to the user's screen
        const screenY = event.screenY; // Y coordinate relative to the user's screen

        // You can also get coordinates relative to the element itself
        const rect = this.getBoundingClientRect(); // 'this' refers to the checkbox element
        const offsetX = event.clientX - rect.left;
        const offsetY = event.clientY - rect.top;

        console.log("User clicked the checkbox!");
        console.log(`Viewport coordinates (clientX, clientY): (${clientX}, ${clientY})`);
        console.log(`Screen coordinates (screenX, screenY): (${screenX}, ${screenY})`);
        console.log(`Coordinates relative to element (offsetX, offsetY): (${offsetX}, ${offsetY})`);

        // If it's a checkbox, you might also want to see its checked state
        console.log(`Checkbox checked state: ${this.checked}`);
    });
} else {
    console.error("Element with ID 'checkbox' not found.");
}
