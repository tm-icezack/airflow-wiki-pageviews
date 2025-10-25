
  <h1>📊 Airflow-wiki-pageviews</h1>



  <h2>📌 Project Title</h2>
  <p>A brief description of what this project does and who it's for.</p>

  <h2>🌀 Airflow Wikipedia Pageviews DAG</h2>

  <h3>📄 Description</h3>
  <p>This Airflow DAG downloads daily Wikipedia pageviews, parses the data, and loads it into a Supabase PostgreSQL database.</p>

  <h3>⚙️ Setup Instructions</h3>

  <h4>1. Install Dependencies</h4>
  <pre><code>pip install -r requirements.txt</code></pre>

  <h4>2. Set Up Airflow Connection</h4>
  <p>Replace <code>&lt;USER&gt;</code> and <code>&lt;PASSWORD&gt;</code> with your Supabase credentials:</p>
  <pre><code>airflow connections add supabase_postgres \
  --conn-type postgres \
  --conn-login &lt;USER&gt; \
  --conn-password &lt;PASSWORD&gt; \
  --conn-host aws-1-eu-north-1.pooler.supabase.com \
  --conn-port 6543 \
  --conn-schema postgres \
  --conn-extra '{"sslmode":"require"}'</code></pre>

  <h4>3. Place DAG File</h4>
  <p>Copy your DAG Python file (<code>wiki_pageviews_dag.py</code>) into the Airflow <code>dags/</code> folder.</p>

  <h4>4. Start Airflow (Docker)</h4>
  <pre><code>docker-compose up -d</code></pre>

  <h4>5. Trigger the DAG Manually (Optional)</h4>
  <p>You can trigger it via the Airflow UI or CLI:</p>
  <pre><code>airflow dags trigger wiki_pageviews_dump_to_supabase</code></pre>

  <h3>🧾 Notes</h3>
  <ul>
    <li>Requires <strong>Python 3.8+</strong> and <strong>Airflow 2.7+</strong></li>
    <li>Ensure your Supabase database is running and reachable</li>
    <li>Verify that your credentials are correct</li>
    <li>The DAG automatically downloads the <strong>previous day's pageviews</strong> and stores them in Supabase</li>
  </ul>

</body>
</html>
