(() => {
    ctx = document.getElementById('question1').getContext('2d');

    const drawChart = (data) => {
        new Chart(ctx, {
            type: 'bar',

            data: {
                labels: data[0],
                datasets: [{
                    label: "Nombre de vente",
                    borderColor: "#c6e2d2",
                    backgroundColor: "#30aeae",
                    data: data[1],
                }]
            },

            // Configuration options go here
            options: {
                title: {
                    display: true,
                    text: 'Catégories des produits les plus vendus'
                },
                scales: {
                    yAxes: [{
                        ticks: {
                            beginAtZero: true
                        }
                    }],
                    xAxes: [{
                        ticks: {
                            autoSkip: true
                        }
                    }]
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
                data.push(parseInt(tableLine[1]));
            }
        });
        console.log(data);
        return [label, data];
    };

    const loadData = () => {
        $.get('../../output/question1/part-r-00000', function (data) {
            drawChart(processData(data.split('\n')))
        });
    };

    loadData();
})();