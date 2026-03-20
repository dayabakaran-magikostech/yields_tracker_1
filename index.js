require("dotenv").config();
const axios = require("axios");

/****************************************
 * CONFIG
 ****************************************/
const config = {
	order_logs_sheet_name: "premium_watcher_exp_1",
	orderTag: "exp_1",
	synthDataLogs: [],
	entryExitOrderLogs: [],
	logsFlusherInterval: 1000 * 60
};

/****************************************
 * order_config
 * Now option_type is explicitly provided
 ****************************************/
const orderConfig = [
	{
		instrument: 16019202,
		trading_symbol: "NIFTY2632423100CE",
		option_type: "CE",
		entry_transaction: "buy",
		expiry: "2026-03-24",
		strike: 23100,
		entry_price: 235,
	},
	{
		instrument: 16019714,
		trading_symbol: "NIFTY2632423100PE",
		option_type: "PE",
		entry_transaction: "sell",
		expiry: "2026-03-24",
		strike: 23100,
		entry_price: 270,
	},
	{
		instrument: 16109058,
		trading_symbol: "NIFTY26MAR25000CE",
		option_type: "CE",
		entry_transaction: "sell",
		expiry: "2026-03-30",
		strike: 25000,
		entry_price: 9.9,
	},
	{
		instrument: 16109314,
		trading_symbol: "NIFTY26MAR25000PE",
		option_type: "PE",
		entry_transaction: "buy",
		expiry: "2026-03-30",
		strike: 25000,
		entry_price: 1910,
	},
];

/****************************************
 * SHEET LOGGER
 ****************************************/
async function addData(s_name, data, url, type = "create") {
	const requestData = {
		type,
		s_name,
		data,
	};

	try {
		const response = await axios.post(url, requestData, {
			headers: { "Content-Type": "application/json" },
		});

		if (response.status === 200 || response.status === 201) {
			return response.data;
		}

		console.error("Request failed with status code:", response.status);
		return false;
	} catch (error) {
		console.error(error);
		return false;
	}
}

async function gs_logs_flusher() {
	try {
		if (config.synthDataLogs.length > 0) {
			const data_to_upload = [];

			while (config.synthDataLogs.length > 0) {
				data_to_upload.push(config.synthDataLogs.shift());
			}

			await addData(
				config.order_logs_sheet_name,
				data_to_upload,
				process.env.CRUD_MICROSERVICE_URL
			);

			console.log("Data added to sheet ...");
		}

		if (config.entryExitOrderLogs.length > 0) {
			const data_to_upload = [];

			while (config.entryExitOrderLogs.length > 0) {
				data_to_upload.push(config.entryExitOrderLogs.shift());
			}

			console.log("Entry/Exit data added to sheet ...");
		}
	} catch (error) {
		console.log("Error occurred in gs_logs_flusher(): ", error.stack);
	}
}

/****************************************
 * HELPERS
 ****************************************/
function chunkArray(arr, size) {
	const output = [];
	for (let i = 0; i < arr.length; i += size) {
		output.push(arr.slice(i, i + size));
	}
	return output;
}

function isValidNumber(val) {
	return typeof val === "number" && !Number.isNaN(val);
}

function roundTo(num, places = 2) {
	if (num == null || Number.isNaN(num)) return null;
	return Number(Number(num).toFixed(places));
}

function normalizeOptionType(optionType) {
	return String(optionType || "").trim().toUpperCase();
}

function normalizeTransactionType(txn) {
	return String(txn || "").trim().toLowerCase();
}

function validateSingleLegAssignment(existingLeg, newLeg, label) {
	if (existingLeg) {
		throw new Error(
			`Duplicate leg found for ${label}. Existing=${existingLeg.trading_symbol}, New=${newLeg.trading_symbol}`
		);
	}
}

function validatePairConsistency(ceLeg, peLeg, pairName) {
	if (!ceLeg || !peLeg) return;

	if (String(ceLeg.expiry) !== String(peLeg.expiry)) {
		throw new Error(
			`${pairName} expiry mismatch: CE=${ceLeg.expiry}, PE=${peLeg.expiry}`
		);
	}

	if (Number(ceLeg.strike) !== Number(peLeg.strike)) {
		throw new Error(
			`${pairName} strike mismatch: CE=${ceLeg.strike}, PE=${peLeg.strike}`
		);
	}
}

/****************************************
 * FETCH KITE QUOTES
 ****************************************/
async function fetchKiteQuotes(tradingSymbols, { apiKey, accessToken, exchange = "NFO" }) {
	const allQuotes = {};
	const chunks = chunkArray([...new Set(tradingSymbols)], 200);

	for (const symbolChunk of chunks) {
		const params = new URLSearchParams();

		for (const tradingSymbol of symbolChunk) {
			params.append("i", `${exchange}:${tradingSymbol}`);
		}

		const url = `https://api.kite.trade/quote?${params.toString()}`;

		const response = await axios.get(url, {
			headers: {
				"X-Kite-Version": "3",
				Authorization: `token ${apiKey}:${accessToken}`,
			},
		});

		if (!response.data || !response.data.data) {
			throw new Error("Invalid quote response from Kite");
		}

		Object.assign(allQuotes, response.data.data);
	}

	return allQuotes;
}

/****************************************
 * MAIN SYNTH BUILDER
 * Treat entire orderConfig as one group
 ****************************************/
async function buildSynthPricingSingleGroup(orderRows, kiteCreds) {
	if (!Array.isArray(orderRows) || orderRows.length === 0) {
		throw new Error("orderRows is empty");
	}

	/**************************************
	 * Step 1: Normalize + validate
	 **************************************/
	const normalizedRows = orderRows.map((row, idx) => {
		const trading_symbol = String(row.trading_symbol || "").trim();
		const option_type = normalizeOptionType(row.option_type);
		const entry_transaction = normalizeTransactionType(row.entry_transaction);
		const expiry = String(row.expiry || "").trim();
		const strike = Number(row.strike);
		const entry_price = Number(row.entry_price);

		if (!trading_symbol) {
			throw new Error(`Missing trading_symbol at row index ${idx}`);
		}

		if (!["CE", "PE"].includes(option_type)) {
			throw new Error(`Invalid option_type for ${trading_symbol}: ${row.option_type}`);
		}

		if (!["buy", "sell"].includes(entry_transaction)) {
			throw new Error(
				`Invalid entry_transaction for ${trading_symbol}: ${row.entry_transaction}`
			);
		}

		if (!expiry) {
			throw new Error(`Missing expiry for ${trading_symbol}`);
		}

		if (!isValidNumber(strike)) {
			throw new Error(`Invalid strike for ${trading_symbol}`);
		}

		if (!isValidNumber(entry_price)) {
			throw new Error(`Invalid entry_price for ${trading_symbol}`);
		}

		return {
			...row,
			trading_symbol,
			option_type,
			entry_transaction,
			expiry,
			strike,
			entry_price,
		};
	});

	/**************************************
	 * Step 2: Fetch quotes
	 **************************************/
	const tradingSymbols = normalizedRows.map((r) => r.trading_symbol);
	const quotesMap = await fetchKiteQuotes(tradingSymbols, kiteCreds);

	/**************************************
	 * Step 3: Attach exit prices
	 **************************************/
	for (const row of normalizedRows) {
		const quoteKey = `${kiteCreds.exchange || "NFO"}:${row.trading_symbol}`;
		const quote = quotesMap[quoteKey];

		if (!quote) {
			row.exit_price = null;
			row.quote_missing = true;
			continue;
		}

		const offerDepth = quote.depth?.sell || [];
		const bidDepth = quote.depth?.buy || [];

		// Worst exit logic:
		// entry buy  => exit sell => use ask => depth.buy[0].price //bidDepth
		// entry sell => exit buy  => use bid => depth.sell[0].price //offerDepth
		if (row.entry_transaction === "buy") {
			row.exit_price =
				bidDepth[0] && bidDepth[0].price != null
					? Number(bidDepth[0].price)
					: null;
		} else {
			row.exit_price =
				offerDepth[0] && offerDepth[0].price != null
					? Number(offerDepth[0].price)
					: null;
		}

		row.quote_missing = row.exit_price == null;
	}

	/**************************************
	 * Step 4: Treat all rows as one group
	 **************************************/
	const grouped = {
		buy_synth_ce: null,   // CE buy
		buy_synth_pe: null,   // PE sell
		sell_synth_ce: null,  // CE sell
		sell_synth_pe: null,  // PE buy
	};

	for (const row of normalizedRows) {
		if (row.option_type === "CE" && row.entry_transaction === "buy") {
			validateSingleLegAssignment(grouped.buy_synth_ce, row, "buy_synth_ce");
			grouped.buy_synth_ce = row;
		}
		else if (row.option_type === "PE" && row.entry_transaction === "sell") {
			validateSingleLegAssignment(grouped.buy_synth_pe, row, "buy_synth_pe");
			grouped.buy_synth_pe = row;
		}
		else if (row.option_type === "CE" && row.entry_transaction === "sell") {
			validateSingleLegAssignment(grouped.sell_synth_ce, row, "sell_synth_ce");
			grouped.sell_synth_ce = row;
		}
		else if (row.option_type === "PE" && row.entry_transaction === "buy") {
			validateSingleLegAssignment(grouped.sell_synth_pe, row, "sell_synth_pe");
			grouped.sell_synth_pe = row;
		}
	}

	/**************************************
	 * Step 5: Validate pair consistency
	 **************************************/
	validatePairConsistency(
		grouped.buy_synth_ce,
		grouped.buy_synth_pe,
		"buy_synth"
	);

	validatePairConsistency(
		grouped.sell_synth_ce,
		grouped.sell_synth_pe,
		"sell_synth"
	);

	/**************************************
	 * Step 6: Extract values
	 **************************************/
	const buyCe = grouped.buy_synth_ce;
	const buyPe = grouped.buy_synth_pe;
	const sellCe = grouped.sell_synth_ce;
	const sellPe = grouped.sell_synth_pe;

	const buySynthStrike =
		buyCe?.strike ?? buyPe?.strike ?? null;
	const sellSynthStrike =
		sellCe?.strike ?? sellPe?.strike ?? null;

	const buySynthExpiry =
		buyCe?.expiry ?? buyPe?.expiry ?? null;
	const sellSynthExpiry =
		sellCe?.expiry ?? sellPe?.expiry ?? null;

	const buy_synth_ce_entry_price = buyCe?.entry_price ?? null;
	const buy_synth_ce_exit_price = buyCe?.exit_price ?? null;

	const buy_synth_pe_entry_price = buyPe?.entry_price ?? null;
	const buy_synth_pe_exit_price = buyPe?.exit_price ?? null;

	const sell_synth_ce_entry_price = sellCe?.entry_price ?? null;
	const sell_synth_ce_exit_price = sellCe?.exit_price ?? null;

	const sell_synth_pe_entry_price = sellPe?.entry_price ?? null;
	const sell_synth_pe_exit_price = sellPe?.exit_price ?? null;

	/**************************************
	 * Step 7: Compute synths
	 **************************************/
	const buy_synth =
		isValidNumber(buySynthStrike) &&
			isValidNumber(buy_synth_ce_exit_price) &&
			isValidNumber(buy_synth_pe_exit_price)
			? roundTo(buySynthStrike + buy_synth_ce_exit_price - buy_synth_pe_exit_price)
			: null;

	const sell_synth =
		isValidNumber(sellSynthStrike) &&
			isValidNumber(sell_synth_ce_exit_price) &&
			isValidNumber(sell_synth_pe_exit_price)
			? roundTo(sellSynthStrike + sell_synth_ce_exit_price - sell_synth_pe_exit_price)
			: null;

	const premium =
		isValidNumber(sell_synth) && isValidNumber(buy_synth)
			? roundTo(sell_synth - buy_synth)
			: null;

	/**************************************
	 * Step 8: Final single row
	 **************************************/
	const outputRow = {
		order_tag: config.orderTag,

		buy_synth_expiry: buySynthExpiry,
		buy_synth_strike: buySynthStrike,

		sell_synth_expiry: sellSynthExpiry,
		sell_synth_strike: sellSynthStrike,

		buy_synth_ce_entry_price,
		buy_synth_ce_exit_price,
		buy_synth_pe_entry_price,
		buy_synth_pe_exit_price,

		sell_synth_ce_entry_price,
		sell_synth_ce_exit_price,
		sell_synth_pe_entry_price,
		sell_synth_pe_exit_price,

		buy_synth,
		sell_synth,
		premium,
	};

	return {
		normalizedRows,
		outputRows: [outputRow],
	};
}

/****************************************
 * MAIN RUNNER
 ****************************************/
async function processSynthAndPushToSheet() {
	try {
		const kiteCreds = {
			apiKey: process.env.KITE_API_KEY,
			accessToken: process.env.KITE_ACCESS_TOKEN,
			exchange: "NFO",
		};

		const { outputRows } = await buildSynthPricingSingleGroup(orderConfig, kiteCreds);

		console.log("Final output rows:");
		console.dir(outputRows, { depth: null });

		config.synthDataLogs.push(...outputRows);

		gs_logs_flusher();

		return outputRows;
	} catch (error) {
		console.error("Error in processSynthAndPushToSheet():", error.stack);
		throw error;
	}
}

/****************************************
 * RUN
 ****************************************/
(async () => {
	try {
		processSynthAndPushToSheet();
		config.logsFlusherInterval = setInterval(processSynthAndPushToSheet, config.logsFlusherInterval);
		// await processSynthAndPushToSheet();
	} catch (error) {
		console.error(error);
	}
})();