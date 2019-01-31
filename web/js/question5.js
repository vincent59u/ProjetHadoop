(() => {
    var mymap = L.map('question5').setView([-10.5480272,-49.0446606], 4);
    L.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token={accessToken}', {
        attribution: 'Map data &copy; <a href="https://www.openstreetmap.org/">OpenStreetMap</a> contributors, <a href="https://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="https://www.mapbox.com/">Mapbox</a>',
        maxZoom: 18,
        id: 'mapbox.streets',
        accessToken: 'pk.eyJ1IjoibWF0dGhpZXV2aW5jZW50MyIsImEiOiJjanJrZ2tuNmMwbGRlNGFucmttZWg0MWNmIn0.6-OkwEJMMC8K7oQ_akoVAQ'
    }).addTo(mymap);


    const drawCircles = (localisationArray) => {
        let i = 0;
        localisationArray.forEach(line => {
            if(line !== ""){
                let [latitude, longitude, commande] = line.split(',');
                L.circle([latitude, longitude], {
                    color: (i < 3) ? 'green' : 'red',
                    fillColor: (i < 3) ? 'green' : 'red',
                    fillOpacity: 0.3,
                    radius: 100000
                }).bindPopup(`Nombre de commande : ${commande}`)
                .on('mouseover', function (e) {
                    this.openPopup();
                })
                .on('mouseout', function (e) {
                    this.closePopup();
                }).addTo(mymap);
            }
            i += 1
        })
    };

    const getAndDrawData = () => {
        fetch('../../output/question5/part-r-00000')
            .then(response => response.text())
            .then(text => drawCircles(text.split('\n')))
    };

    getAndDrawData()
})();