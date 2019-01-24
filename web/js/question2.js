(() => {
    ctx = document.getElementById('question2').getContext('2d');

    const drawChart = (data) => {
        new Chart(ctx, {
            type: 'line',

            // The data for our dataset
            data: {
                labels: data[0],
                datasets: [{
                    label: "Moyenne des frais de ports",
                    fill: false,
                    borderColor: "#30aeae",
                    backgroundColor: "#c6e2d2",
                    data: data[1],
                }]
            },

            // Configuration options go here
            options: {
                title: {
                    display: true,
                    text: 'Moyenne des frais de ports en fonction de la distance de livraison (en Km)'
                }
            }
        });
    };

    const processData = lines => {
        label = [];
        data = [];
        lines.forEach(line => {
            if(line !== "")
            {
                const tableLine = line.split(',');
                label.push(tableLine[0]);
                data.push(parseFloat(tableLine[1]).toFixed(3));
            }
        });
        return [label, data];
    };

    const loadData = () => {
        $.get('../../output/question2/part-r-00000', function (data) {
            drawChart(processData(data.split('\n')))
        });
    };

    loadData();
})();