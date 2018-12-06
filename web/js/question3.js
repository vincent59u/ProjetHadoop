(() => {
    ctx = document.getElementById('question3').getContext('2d');

    const drawChart = (data) => {
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
                const tableLine = line.split('\t');
                label.push(tableLine[0]);
                data.push(parseFloat(tableLine[1]).toFixed(3));
            }
        });
        return [label, data];
    };

    const loadData = () => {
        $.get('../../output/question3/part-r-00000', function (data) {
            drawChart(processData(data.split('\n')))
        });
    };

    loadData();
})();