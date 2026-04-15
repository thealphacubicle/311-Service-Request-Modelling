import { ResultsExplorer } from "../../components/ResultsExplorer";
import { getModelResults } from "../../lib/model-results";

export default async function ResultsPage() {
  const rows = await getModelResults();

  return (
    <>
      <section className="card">
        <h2>Method Results</h2>
        <p className="muted">
          These results come from your BRMS grid-search outputs generated in the
          notebooks and stored in `results/metrics`. Use the controls to compare
          how model family, priors, and training version impact predictive
          performance.
        </p>
      </section>
      <ResultsExplorer rows={rows} />
    </>
  );
}
