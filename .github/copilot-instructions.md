This is a monorepo for the AirSupply project, a prototype of a pricing intelligent system designed to help product supplier of the life science industry to enable basket level dynamic pricing.

Project Structure:
- The backend is built using Python. All backend code is located in the `backend` directory.
- The python virtual environment is managed using `uv`. Located at the project directory.

Project-wide Practices:
- All python environment and package management should be done at the project directory.
- When creating new artifacts, ensure they are created at the correct folder. If running a command that creates a new file, ensure you are in the correct directory.
- Use the `uv` command to run the backend server. This ensures that the correct virtual environment is activated.
- Use `uv run` to run the backend server. This command will automatically activate the virtual environment and run the server.
- Use `uv run pytest` to run tests. This ensures that the tests are run in the correct environment.
- Use `uv run pre-commit` to run pre-commit hooks. This ensures that the code is formatted and linted correctly before committing.

Project structure:
The application code are kept under "app" directory. Everything related to the app including the test code, assets and configurations are kept under this directory.

Key pain points of the business problem where the dynamic pricing system targeted to solve:
- The business problem revolves around optimizing revenue and margins in life sciences e-commerce platforms, which act as marketplaces for lab equipment, reagents, and consumables. These platforms face high complexity due to global supply chains, regulatory variations, and data inconsistencies. Based on the problem description, here are the key pain points, categorized for clarity:

- Complex and Static Invoicing Processes: Traditional invoices lack flexibility to incorporate dynamic elements like tariffs (which vary by geography and HS codes), service fees, handling charges, and promotions. This leads to suboptimal revenue capture, as fields don't adapt to transaction types (e.g., bulk orders vs. single items), buyer segments (e.g., academic vs. enterprise), or regional regulations, resulting in lost opportunities for upselling or fee adjustments.

- Inefficient Pricing Strategies: Pricing is often static and not informed by historical data, leading to missed revenue from price elasticity (how demand changes with price), seasonality (e.g., end-of-year budget spending in academia), or promotions. Without customer segmentation, platforms can't tailor prices to groups like biotech startups (price-sensitive) vs. pharma enterprises (volume-driven), causing either eroded margins from over-discounting or lost sales from overpricing.
Inaccurate and Incomplete Shipping Cost Estimation: Shipping fees are a major margin eroder, especially for international baskets with products from multiple suppliers. Key issues include incomplete product metadata (e.g., missing weights or dimensions), which prevents accurate basket-level predictions. This is compounded by variable factors like origin-destination mapping, carrier rates (e.g., UPS, FedEx), and tariffs/duties, leading to underestimation (hurting margins) or overestimation (deterring customers). Handling multi-country sourcing adds logistical complexity, as rules for zones and distances aren't dynamically applied.

- Data Silos and Lack of Intelligence: Historical transaction and invoice data isn't leveraged for learning, making it hard to predict outcomes or simulate scenarios. This results in reactive rather than proactive decision-making, with no real-time insights for pricing teams. Broader impacts include operational inefficiencies, competitive disadvantages against platforms with better logistics, and pressure on margins from rising supplier costs and global trade barriers.

- Scalability and Usability Challenges: With millions of transactions, manual processes can't scale, and fragmented tools (separate systems for pricing, shipping, invoicing) reduce usability for teams, leading to errors and slower operations.

Key elements:
- Dynamic elements in invoicing process: Like tariffs (which vary by geography and HS codes), service fees, handling charges, and promotions shall be incorporated into the invoice dynamically.

- Pricing strategies: lack historical data to optimize revenue from price elasticity, seasonality, and customer segmentation.

- Shipping cost estimation shall leverage complete product metadata and real-time data to improve accuracy and account for variables like carrier rates and international tariffs.

