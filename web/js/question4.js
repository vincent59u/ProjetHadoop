(() => {

    ctx = document.getElementById('question4').getContext('2d');

    const drawChart = (data) => {
        new Chart(ctx, {
            type: 'bar',

            // The data for our dataset
            data: {
                labels: data[0],
                datasets: [{
                    label: "Nombre de produit",
                    backgroundColor: "#30aeae",
                    borderColor: "#c6e2d2",
                    data: data[1],
                }]
            },

            // Configuration options go here
            options: {
                title: {
                    display: true,
                    text: 'Répartition des produits en fonction de leur volume (en cm3)'
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
        return [label, data];
    };

    const loadData = () => {
        $.get('../../output/question4/part-r-00000', function (data) {
            drawChart(processData(data.split('\n')))
        });
    };

    loadData();
})();