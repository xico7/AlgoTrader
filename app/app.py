import sys
sys.path.append('../')
import base64
from io import BytesIO
from flask import Flask, render_template
from matplotlib.figure import Figure
from MongoDB.db_actions import trades_chart, ValidatorDB, DBCol
from data_handling.data_helpers.vars_constants import ONE_DAY_IN_MS

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/search_symbol', methods=['POST'])
def get_symbol_chart():
    symbol = "BTCUSDT"
    chart_tf_db = trades_chart.format(5)
    start_ts = ValidatorDB(chart_tf_db).start_ts
    symbol_chart = DBCol(chart_tf_db, symbol).column_between(start_ts, start_ts + ONE_DAY_IN_MS / 24, 'start_ts')
    start_timestamps = [v['start_ts'] for v in symbol_chart if v['range_price_volume_difference']]
    volumes = [v['range_price_volume_difference']['rise_of_start_end_price_in_percentage'] for v in symbol_chart
               if v['range_price_volume_difference']]
    fig = Figure()
    ax = fig.subplots()
    # Generate the figure **without using pyplot**.
    ax.plot(start_timestamps, volumes)
    # Save it to a temporary buffer.
    buf = BytesIO()
    fig.savefig(buf, format="png")
    # Embed the result in the html output.
    data = base64.b64encode(buf.getbuffer()).decode("ascii")

    return render_template("chart.html", plot_url=data)


@app.route('/test')
def test_matplot():
    # Generate the figure **without using pyplot**.
    fig = Figure()
    ax = fig.subplots()
    ax.plot([1, 2])
    # Save it to a temporary buffer.
    buf = BytesIO()
    fig.savefig(buf, format="png")
    # Embed the result in the html output.
    data = base64.b64encode(buf.getbuffer()).decode("ascii")
    return f"<img src='data:image/png;base64,{data}'/>"