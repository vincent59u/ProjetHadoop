(() => {
    ctx = document.getElementById('question3').getContext('2d');
    ctx_1 = document.getElementById('question3_1').getContext('2d');

    const drawChart = (data) => {
        // 1er chart avec la note moyenne
        new Chart(ctx, {
            type: 'bar',

            data: {
                labels: data[0],
                datasets: [{
                    label: "Note moyenne",
                    backgroundColor: "#30aeae",
                    borderColor: "#30aeae",
                    data: data[1],
                }]
            },

            // Configuration options go here
            options: {
                title: {
                    display: true,
                    text: 'Corrélation entre le délai de livraison et la note de satisfaction'
                },
                scales: {
                    yAxes: [{
                        ticks: {
                            beginAtZero: true
                        }
                    }]
                }
            }
        });

        //2eme chart avec le volume des commande par jours de délai
        new Chart(ctx_1, {
            type: 'line',

            data: {
                labels: data[0],
                datasets: [{
                    label: "Nombre de commande",
                    fill: false,
                    backgroundColor: "#30aeae",
                    borderColor: "#30aeae",
                    data: data[2],
                }]
            },

            // Configuration options go here
            options: {
                title: {
                    display: true,
                    text: 'Nombre de livraison en fonction du délai de livraison'
                },
                scales: {
                    yAxes: [{
                        stacked: true
                    }]
                }
            }
        });
    };

    const processData = lines => {
        label = [];
        data = [];
        data_1 = [];
        lines.forEach(line => {
            if(line !== "")
            {
                const tableLine = line.split(',');
                label.push(tableLine[0]);
                data.push(parseFloat(tableLine[1]).toFixed(3));
                data_1.push(parseInt(tableLine[2]));
            }
        });
        return [label, data, data_1];
    };

    const loadData = () => {
        $.get('../../output/question3/part-r-00000', function (data) {
            drawChart(processData(data.split('\n')))
        });
    };

    loadData();
})();