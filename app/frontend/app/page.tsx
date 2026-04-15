export default function LandingPage() {
  return (
    <>
      <section className="card">
        <h2>Forecasting Boston 311 Demand</h2>
        <p className="muted">
          This project models neighborhood-level 311 service request demand in
          Boston using historical 2019-2024 request data and Bayesian
          count-based models. The goal is to estimate expected service load and
          uncertainty so city teams can allocate operations proactively.
        </p>
      </section>

      <section className="grid-2">
        <article className="card">
          <h3>Data + Features</h3>
          <p className="muted">
            Analyze Boston 311 records (~1.1M rows) were aggregated to daily
            counts and engineered with calendar structure, seasonality, and
            neighborhood factors. Exploratory analysis confirmed
            over-dispersion, motivating negative binomial alternatives to simple
            Poisson baselines.
          </p>
        </article>

        <article className="card">
          <h3>Modeling Approach</h3>
          <p className="muted">
            Multiple Bayesian model variants were compared across prior settings
            and distribution families. Evaluation uses MAE/RMSE for predictive
            error, coverage for interval quality, and WAIC/LOO-IC for model fit
            under out-of-sample assumptions.
          </p>
        </article>
      </section>

      <section className="card">
        <h3>What To Explore Next</h3>
        <p className="muted">
          Open the Interactive Results tab to compare model versions from your
          notebook outputs. You can filter by family/version and switch metrics
          to inspect trade-offs between calibration, fit quality, and training
          cost.
        </p>
      </section>
    </>
  );
}
