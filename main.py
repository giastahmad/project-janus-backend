from flask import Flask, request, jsonify, send_file, render_template
from flask_cors import CORS
from sqlalchemy import func
from datetime import datetime
import os
from dotenv import load_dotenv

from models import OrderFact, ProductDimension, PlatformDimension, DateDimension, LocationDimension, PaymentMethodDimension
from config import SessionLocal
from etl import extract, transform, load

app = Flask(__name__)
CORS(app)

load_dotenv()
UPLOAD_FOLDER = './data/uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


# ==========================================
# KONFIGURASI KEAMANAN URL
# ==========================================
def is_authorized(req):
    token_di_header = req.headers.get('X-API-KEY')
    token_di_url    = req.args.get('key')
    secret_key      = os.getenv('X-API-KEY')
    return token_di_header == secret_key or token_di_url == secret_key


# ==========================================
# INDEX
# ==========================================
@app.route('/', methods=['GET'])
def index():
    return jsonify({"message": "API Backend Aktif dan Berjalan Sempurna!"}), 200


# ==========================================
# 1. UPLOAD
# ==========================================
@app.route('/api/upload', methods=['POST'])
def upload_data():
    if not is_authorized(request):
        return jsonify({"message": "Akses Ditolak: URL tidak valid atau tidak memiliki izin."}), 401

    if 'file' not in request.files:
        return jsonify({"message": "Tidak ada file yang dikirim"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"message": "Nama file kosong"}), 400

    filepath = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
    file.save(filepath)

    try:
        # extract.run_extract(filepath)
        # transform.run_transform()
        # load.run_load_to_mysql()
        return jsonify({"message": "File berhasil diproses."}), 200
    except Exception as e:
        return jsonify({"message": f"Terjadi kesalahan sistem: {str(e)}"}), 500


# ==========================================
# 2. DASHBOARD METRICS
# ==========================================
def get_dashboard_metrics(start_date=None, end_date=None, platform=None):
    session = SessionLocal()
    try:
        base_filter = []
        if start_date:
            base_filter.append(DateDimension.date >= start_date)
        if end_date:
            base_filter.append(DateDimension.date <= end_date)
        if platform:
            base_filter.append(PlatformDimension.platform_name == platform)

        def _base(cols):
            """Query dengan anchor OrderFact, join date & platform, apply filter."""
            return (
                session.query(*cols)
                .select_from(OrderFact)
                .join(DateDimension,     OrderFact.date_id     == DateDimension.date_id)
                .join(PlatformDimension, OrderFact.platform_id == PlatformDimension.platform_id)
                .filter(*base_filter)
            )

        # --------------------------------------------------
        # 1. Daftar semua platform (tidak difilter, untuk dropdown)
        # --------------------------------------------------
        all_platforms = (
            session.query(PlatformDimension.platform_name)
            .order_by(PlatformDimension.platform_name)
            .all()
        )
        platform_options = [p[0] for p in all_platforms]

        # --------------------------------------------------
        # 2. KPI: Jumlah Model
        # --------------------------------------------------
        num_models = (
            _base([func.count(func.distinct(ProductDimension.product_model))])
            .join(ProductDimension, OrderFact.product_id == ProductDimension.product_id)
            .scalar() or 0
        )

        # --------------------------------------------------
        # 3. KPI: Total Order
        # --------------------------------------------------
        num_orders = (
            _base([func.count(func.distinct(OrderFact.order_key))])
            .scalar() or 0
        )

        # --------------------------------------------------
        # 4. KPI: Total Pendapatan
        # --------------------------------------------------
        revenue_total = (
            _base([func.sum(OrderFact.total_amount)])
            .scalar() or 0
        )

        # --------------------------------------------------
        # 5. KPI: Jumlah Kota
        # --------------------------------------------------
        num_cities = (
            _base([func.count(func.distinct(LocationDimension.city))])
            .join(LocationDimension, OrderFact.location_id == LocationDimension.location_id)
            .scalar() or 0
        )

        # --------------------------------------------------
        # 6. KPI: AOV
        # --------------------------------------------------
        aov = (revenue_total / num_orders) if num_orders > 0 else 0

        # --------------------------------------------------
        # 7. KPI: Ramadhan vs Normal
        # --------------------------------------------------
        ramadhan_filter = base_filter + [DateDimension.is_ramadhan == 1]
        normal_filter   = base_filter + [DateDimension.is_ramadhan == 0]

        def _ramadhan(cols):
            return (
                session.query(*cols)
                .select_from(OrderFact)
                .join(DateDimension,     OrderFact.date_id     == DateDimension.date_id)
                .join(PlatformDimension, OrderFact.platform_id == PlatformDimension.platform_id)
                .filter(*ramadhan_filter)
            )

        def _normal(cols):
            return (
                session.query(*cols)
                .select_from(OrderFact)
                .join(DateDimension,     OrderFact.date_id     == DateDimension.date_id)
                .join(PlatformDimension, OrderFact.platform_id == PlatformDimension.platform_id)
                .filter(*normal_filter)
            )

        ramadhan_days = _ramadhan([func.count(func.distinct(DateDimension.date))]).scalar() or 0
        normal_days   = _normal([func.count(func.distinct(DateDimension.date))]).scalar() or 0

        ramadhan_revenue = _ramadhan([func.sum(OrderFact.total_amount)]).scalar() or 0
        normal_revenue   = _normal([func.sum(OrderFact.total_amount)]).scalar() or 0

        ramadhan_avg_revenue = (ramadhan_revenue / ramadhan_days) if ramadhan_days > 0 else 0
        normal_avg_revenue   = (normal_revenue   / normal_days)   if normal_days   > 0 else 0

        ramadhan_orders = _ramadhan([func.count(func.distinct(OrderFact.order_key))]).scalar() or 0
        normal_orders   = _normal([func.count(func.distinct(OrderFact.order_key))]).scalar() or 0

        ramadhan_avg_orders = (ramadhan_orders / ramadhan_days) if ramadhan_days > 0 else 0
        normal_avg_orders   = (normal_orders   / normal_days)   if normal_days   > 0 else 0

        if normal_avg_revenue > 0:
            ramadhan_lift = round(
                (ramadhan_avg_revenue - normal_avg_revenue) / normal_avg_revenue * 100, 1
            )
        else:
            ramadhan_lift = 0

        # --------------------------------------------------
        # 8. Chart: Bar — Quantity per Warna
        # --------------------------------------------------
        color_data = (
            _base([ProductDimension.product_color, func.sum(OrderFact.quantity)])
            .join(ProductDimension, OrderFact.product_id == ProductDimension.product_id)
            .group_by(ProductDimension.product_color)
            .order_by(func.sum(OrderFact.quantity).desc())
            .all()
        )

        # --------------------------------------------------
        # 9. Chart: Line — Penjualan per Platform per Tanggal
        # --------------------------------------------------
        line_data = (
            _base([DateDimension.date, PlatformDimension.platform_name, func.sum(OrderFact.total_amount)])
            .group_by(DateDimension.date, PlatformDimension.platform_name)
            .order_by(DateDimension.date)
            .all()
        )

        # --------------------------------------------------
        # 10. Top 5 Best Selling Models
        # --------------------------------------------------
        top_products = (
            _base([ProductDimension.product_model, func.sum(OrderFact.quantity)])
            .join(ProductDimension, OrderFact.product_id == ProductDimension.product_id)
            .group_by(ProductDimension.product_model)
            .order_by(func.sum(OrderFact.quantity).desc())
            .limit(5)
            .all()
        )

        # --------------------------------------------------
        # 11. Map: Persebaran per Provinsi
        # --------------------------------------------------
        map_query = (
            _base([LocationDimension.province, func.sum(OrderFact.quantity)])
            .join(LocationDimension, OrderFact.location_id == LocationDimension.location_id)
            .group_by(LocationDimension.province)
            .all()
        )
        
        # --------------------------------------------------
        # 12. Chart: Avg Basket Size per Payment Method
        # --------------------------------------------------
        payment_data = (
            _base([PaymentMethodDimension.payment_method_name, func.avg(OrderFact.total_amount)])
            .join(PaymentMethodDimension, OrderFact.payment_method_id == PaymentMethodDimension.payment_method_id)
            .group_by(PaymentMethodDimension.payment_method_name)
            .order_by(func.avg(OrderFact.total_amount).desc())
            .all()
        )

        # --------------------------------------------------
        # 13. Chart: Product Size Distribution
        # --------------------------------------------------
        size_data = (
            _base([ProductDimension.product_size, func.count(OrderFact.order_key)])
            .join(ProductDimension, OrderFact.product_id == ProductDimension.product_id)
            .group_by(ProductDimension.product_size)
            .order_by(func.count(OrderFact.order_key).desc())
            .all()
        )

        map_data = [["Provinsi", "Total Terjual"]]
        for row in map_query:
            map_data.append([row[0], int(row[1])])

        # --------------------------------------------------
        # Return
        # --------------------------------------------------
        return {
            "platform_options":      platform_options,
            "num_models":            num_models,
            "num_orders":            num_orders,
            "revenue_month":         float(revenue_total),
            "num_cities":            num_cities,
            "aov":                   round(float(aov), 0),
            "ramadhan_lift":         ramadhan_lift,
            "ramadhan_avg_revenue":  round(float(ramadhan_avg_revenue), 0),
            "normal_avg_revenue":    round(float(normal_avg_revenue), 0),
            "ramadhan_avg_orders":   round(float(ramadhan_avg_orders), 2),
            "normal_avg_orders":     round(float(normal_avg_orders), 2),
            "map_data":              map_data,
            "color_labels":          [c[0] for c in color_data],
            "color_values":          [int(c[1]) for c in color_data],
            "line_data":             [
                {"date": str(d[0]), "platform": d[1], "amount": float(d[2])}
                for d in line_data
            ],
            "top_products":          [[p[0], int(p[1])] for p in top_products],
            "payment_labels":        [p[0] for p in payment_data],
            "payment_values":        [float(p[1]) for p in payment_data],
            "size_labels":           [s[0] for s in size_data],
            "size_values":           [int(s[1]) for s in size_data],
        }

    finally:
        session.close()


@app.route('/dashboard')
def dashboard_view():
    if request.args.get('key') != os.getenv('X-API-KEY'):
        return "Akses Ditolak", 401

    start_date = request.args.get('start_date')
    end_date   = request.args.get('end_date')
    platform   = request.args.get('platform')

    metrics = get_dashboard_metrics(start_date, end_date, platform)
    return render_template('dashboard.html', m=metrics)


# ==========================================
# 3. PREDIKSI
# ==========================================
@app.route('/api/download-prediction', methods=['GET'])
def download_prediction():
    if not is_authorized(request):
        return jsonify({"message": "Akses Ditolak"}), 401

    path_to_excel = "./ml_models/forecast_sku_mei_2026.xlsx"
    try:
        return send_file(path_to_excel, as_attachment=True)
    except Exception:
        return jsonify({"message": "File laporan tidak ditemukan"}), 404


if __name__ == '__main__':
    app.run(debug=True, port=5000)