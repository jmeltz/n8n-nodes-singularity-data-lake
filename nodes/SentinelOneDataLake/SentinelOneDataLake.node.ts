import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	IHttpRequestMethods,
	IRequestOptions,
	IDataObject,
	NodeOperationError,
} from 'n8n-workflow';

export class SentinelOneDataLake implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'SentinelOne Singularity Data Lake',
		name: 'sentinelOneDataLake',
		icon: 'file:sentinelone.png',
		group: ['transform'],
		version: 1,
		subtitle: '={{$parameter["operation"] + ": " + $parameter["resource"]}}',
		description: 'Interact with the SentinelOne Singularity Data Lake API',
		defaults: {
			name: 'SentinelOne Data Lake',
		},
		inputs: ['main'],
		outputs: ['main'],
		credentials: [
			{
				name: 'sentinelOneDataLakeApi',
				required: true,
			},
		],
		properties: [
			// Resource selection
			{
				displayName: 'Resource',
				name: 'resource',
				type: 'options',
				noDataExpression: true,
				options: [
					{
						name: 'Query',
						value: 'query',
						description: 'Search and retrieve events from the data lake',
					},
					{
						name: 'Event',
						value: 'event',
						description: 'Insert events into the data lake',
					},
				],
				default: 'query',
			},

			// ==========================================
			// Query Resource Operations
			// ==========================================
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: {
					show: {
						resource: ['query'],
					},
				},
				options: [
					{
						name: 'Search Events',
						value: 'search',
						description: 'Get events (log records) that match your query',
						action: 'Search events',
					},
					{
						name: 'Facet Query',
						value: 'facet',
						description: 'Get the most frequent values of a field',
						action: 'Get facet values',
					},
					{
						name: 'Timeseries Query',
						value: 'timeseries',
						description: 'Get numeric/graph data for single or multiple queries',
						action: 'Get timeseries data',
					},
					{
						name: 'Power Query',
						value: 'powerQuery',
						description: 'Execute a PowerQuery to transform, group, and summarize data',
						action: 'Execute power query',
					},
				],
				default: 'search',
			},

			// ==========================================
			// Event Resource Operations
			// ==========================================
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: {
					show: {
						resource: ['event'],
					},
				},
				options: [
					{
						name: 'Upload Logs',
						value: 'uploadLogs',
						description: 'Upload unstructured, plain-text logs',
						action: 'Upload logs',
					},
					{
						name: 'Add Events',
						value: 'addEvents',
						description: 'Insert structured or unstructured log events',
						action: 'Add events',
					},
				],
				default: 'uploadLogs',
			},

			// ==========================================
			// Search Events Fields
			// ==========================================
			{
				displayName: 'Filter',
				name: 'filter',
				type: 'string',
				default: '',
				placeholder: 'serverHost contains "frontend"',
				description: 'A search expression to get matching events. Leave empty to match all events.',
				displayOptions: {
					show: {
						resource: ['query'],
						operation: ['search'],
					},
				},
			},
			{
				displayName: 'Start Time',
				name: 'startTime',
				type: 'string',
				default: '24h',
				placeholder: '24h, 7d, or 2024-01-01 12:00',
				description: 'Start time for the query. Supports relative (24h, 7d) or absolute formats.',
				displayOptions: {
					show: {
						resource: ['query'],
						operation: ['search'],
					},
				},
			},
			{
				displayName: 'End Time',
				name: 'endTime',
				type: 'string',
				default: '',
				placeholder: 'Leave empty for current time',
				description: 'End time for the query. Defaults to the current time if not set.',
				displayOptions: {
					show: {
						resource: ['query'],
						operation: ['search'],
					},
				},
			},
			{
				displayName: 'Additional Options',
				name: 'additionalOptions',
				type: 'collection',
				placeholder: 'Add Option',
				default: {},
				displayOptions: {
					show: {
						resource: ['query'],
						operation: ['search'],
					},
				},
				options: [
					{
						displayName: 'Max Count',
						name: 'maxCount',
						type: 'number',
						default: 100,
						typeOptions: {
							minValue: 1,
							maxValue: 5000,
						},
						description: 'Maximum number of events to return (1-5000)',
					},
					{
						displayName: 'Page Mode',
						name: 'pageMode',
						type: 'options',
						options: [
							{
								name: 'Head (Oldest First)',
								value: 'head',
							},
							{
								name: 'Tail (Newest First)',
								value: 'tail',
							},
						],
						default: 'head',
						description: 'Whether to get the oldest or newest events when results exceed maxCount',
					},
					{
						displayName: 'Columns',
						name: 'columns',
						type: 'string',
						default: '',
						placeholder: 'severity, serverHost, message',
						description: 'Comma-delimited list of fields to include for each event',
					},
					{
						displayName: 'Continuation Token',
						name: 'continuationToken',
						type: 'string',
						default: '',
						description: 'Token from previous query to get next page of results',
					},
					{
						displayName: 'Priority',
						name: 'priority',
						type: 'options',
						options: [
							{
								name: 'Low',
								value: 'low',
							},
							{
								name: 'High',
								value: 'high',
							},
						],
						default: 'low',
						description: 'Query execution priority. Low has more generous rate limits.',
					},
					{
						displayName: 'Team Emails',
						name: 'teamEmails',
						type: 'string',
						default: '',
						placeholder: 'team1@example.com, team2@example.com',
						description: 'Comma-separated list of account emails for Cross Team Search',
					},
					{
						displayName: 'Tenant',
						name: 'tenant',
						type: 'boolean',
						default: false,
						description: 'Whether to query all accounts accessible to the user',
					},
					{
						displayName: 'Account IDs',
						name: 'accountIds',
						type: 'string',
						default: '',
						placeholder: '123456789, 987654321',
						description: 'Comma-separated list of Console account IDs for Cross Team Search',
					},
				],
			},

			// ==========================================
			// Facet Query Fields
			// ==========================================
			{
				displayName: 'Field',
				name: 'facetField',
				type: 'string',
				required: true,
				default: '',
				placeholder: 'ip, status, serverHost',
				description: 'The field to get the most frequent values of',
				displayOptions: {
					show: {
						resource: ['query'],
						operation: ['facet'],
					},
				},
			},
			{
				displayName: 'Filter',
				name: 'facetFilter',
				type: 'string',
				default: '',
				placeholder: 'serverHost contains "frontend"',
				description: 'A search expression to filter events before counting field values',
				displayOptions: {
					show: {
						resource: ['query'],
						operation: ['facet'],
					},
				},
			},
			{
				displayName: 'Start Time',
				name: 'facetStartTime',
				type: 'string',
				required: true,
				default: '24h',
				placeholder: '24h, 7d, or 2024-01-01 12:00',
				description: 'Start time for the query (required)',
				displayOptions: {
					show: {
						resource: ['query'],
						operation: ['facet'],
					},
				},
			},
			{
				displayName: 'Additional Options',
				name: 'facetOptions',
				type: 'collection',
				placeholder: 'Add Option',
				default: {},
				displayOptions: {
					show: {
						resource: ['query'],
						operation: ['facet'],
					},
				},
				options: [
					{
						displayName: 'End Time',
						name: 'endTime',
						type: 'string',
						default: '',
						description: 'End time for the query. Defaults to the current time.',
					},
					{
						displayName: 'Max Count',
						name: 'maxCount',
						type: 'number',
						default: 100,
						typeOptions: {
							minValue: 1,
							maxValue: 1000,
						},
						description: 'Maximum number of field values to return (1-1000)',
					},
					{
						displayName: 'Priority',
						name: 'priority',
						type: 'options',
						options: [
							{
								name: 'Low',
								value: 'low',
							},
							{
								name: 'High',
								value: 'high',
							},
						],
						default: 'low',
						description: 'Query execution priority',
					},
				],
			},

			// ==========================================
			// Timeseries Query Fields
			// ==========================================
			{
				displayName: 'Queries',
				name: 'timeseriesQueries',
				type: 'fixedCollection',
				typeOptions: {
					multipleValues: true,
				},
				default: {},
				placeholder: 'Add Query',
				displayOptions: {
					show: {
						resource: ['query'],
						operation: ['timeseries'],
					},
				},
				options: [
					{
						name: 'queryItems',
						displayName: 'Query',
						values: [
							{
								displayName: 'Filter',
								name: 'filter',
								type: 'string',
								default: '',
								placeholder: 'serverHost contains "frontend"',
								description: 'Search expression for matching events',
							},
							{
								displayName: 'Start Time',
								name: 'startTime',
								type: 'string',
								required: true,
								default: '1h',
								description: 'Start time for the query (required)',
							},
							{
								displayName: 'End Time',
								name: 'endTime',
								type: 'string',
								default: '',
								description: 'End time for the query. Defaults to the current time.',
							},
							{
								displayName: 'Function',
								name: 'function',
								type: 'options',
								options: [
									{
										name: 'Count',
										value: 'count',
										description: 'Count the number of events in each bucket',
									},
									{
										name: 'Rate',
										value: 'rate',
										description: 'Matching events per second',
									},
									{
										name: 'Mean',
										value: 'mean',
										description: 'Mean value of a numeric field',
									},
									{
										name: 'Min',
										value: 'min',
										description: 'Minimum value of a numeric field',
									},
									{
										name: 'Max',
										value: 'max',
										description: 'Maximum value of a numeric field',
									},
									{
										name: 'Sum',
										value: 'sumPerSecond',
										description: 'Sum of values per second',
									},
									{
										name: 'P10',
										value: 'p10',
										description: '10th percentile',
									},
									{
										name: 'P50 (Median)',
										value: 'p50',
										description: '50th percentile (median)',
									},
									{
										name: 'P90',
										value: 'p90',
										description: '90th percentile',
									},
									{
										name: 'P95',
										value: 'p95',
										description: '95th percentile',
									},
									{
										name: 'P99',
										value: 'p99',
										description: '99th percentile',
									},
								],
								default: 'count',
								description: 'Aggregation function to apply to events',
							},
							{
								displayName: 'Buckets',
								name: 'buckets',
								type: 'number',
								default: 60,
								typeOptions: {
									minValue: 1,
									maxValue: 5000,
								},
								description: 'Number of time buckets to divide the query range into',
							},
							{
								displayName: 'Create Summaries',
								name: 'createSummaries',
								type: 'boolean',
								default: true,
								description: 'Whether to create a timeseries of precomputed results for faster future queries',
							},
							{
								displayName: 'Only Use Summaries',
								name: 'onlyUseSummaries',
								type: 'boolean',
								default: false,
								description: 'Whether to only query preexisting timeseries (faster but may have incomplete results)',
							},
							{
								displayName: 'Priority',
								name: 'priority',
								type: 'options',
								options: [
									{
										name: 'Low',
										value: 'low',
									},
									{
										name: 'High',
										value: 'high',
									},
								],
								default: 'low',
								description: 'Query execution priority',
							},
						],
					},
				],
			},

			// ==========================================
			// Power Query Fields
			// ==========================================
			{
				displayName: 'Query',
				name: 'powerQueryExpression',
				type: 'string',
				typeOptions: {
					rows: 5,
				},
				required: true,
				default: '',
				placeholder: 'status >= 100 status <=599 | group count() by status',
				description: 'The PowerQuery expression to execute. Pipe search expressions into commands to transform, group, and summarize data.',
				displayOptions: {
					show: {
						resource: ['query'],
						operation: ['powerQuery'],
					},
				},
			},
			{
				displayName: 'Start Time',
				name: 'powerQueryStartTime',
				type: 'string',
				default: '24h',
				placeholder: '24h, 7d, or 2024-01-01 12:00',
				description: 'Start time for the query',
				displayOptions: {
					show: {
						resource: ['query'],
						operation: ['powerQuery'],
					},
				},
			},
			{
				displayName: 'Additional Options',
				name: 'powerQueryOptions',
				type: 'collection',
				placeholder: 'Add Option',
				default: {},
				displayOptions: {
					show: {
						resource: ['query'],
						operation: ['powerQuery'],
					},
				},
				options: [
					{
						displayName: 'End Time',
						name: 'endTime',
						type: 'string',
						default: '',
						description: 'End time for the query. Defaults to the current time.',
					},
					{
						displayName: 'Priority',
						name: 'priority',
						type: 'options',
						options: [
							{
								name: 'Low',
								value: 'low',
							},
							{
								name: 'High',
								value: 'high',
							},
						],
						default: 'low',
						description: 'Query execution priority',
					},
					{
						displayName: 'Team Emails',
						name: 'teamEmails',
						type: 'string',
						default: '',
						placeholder: 'team1@example.com, team2@example.com',
						description: 'Comma-separated list of account emails for Cross Team Search',
					},
					{
						displayName: 'Tenant',
						name: 'tenant',
						type: 'boolean',
						default: false,
						description: 'Whether to query all accounts accessible to the user',
					},
					{
						displayName: 'Account IDs',
						name: 'accountIds',
						type: 'string',
						default: '',
						placeholder: '123456789, 987654321',
						description: 'Comma-separated list of Console account IDs for Cross Team Search',
					},
				],
			},

			// ==========================================
			// Upload Logs Fields
			// ==========================================
			{
				displayName: 'Log Data',
				name: 'logData',
				type: 'string',
				typeOptions: {
					rows: 5,
				},
				required: true,
				default: '',
				placeholder: 'Log 1: Hello, World!\nLog 2: Hello Again',
				description: 'The log data to upload. Separate each log message with line breaks.',
				displayOptions: {
					show: {
						resource: ['event'],
						operation: ['uploadLogs'],
					},
				},
			},
			{
				displayName: 'Additional Options',
				name: 'uploadLogsOptions',
				type: 'collection',
				placeholder: 'Add Option',
				default: {},
				displayOptions: {
					show: {
						resource: ['event'],
						operation: ['uploadLogs'],
					},
				},
				options: [
					{
						displayName: 'Parser',
						name: 'parser',
						type: 'string',
						default: 'uploadLogs',
						description: 'Parser name for the log configuration file',
					},
					{
						displayName: 'Server Host',
						name: 'serverHost',
						type: 'string',
						default: '',
						description: 'The hostname or server identifier',
					},
					{
						displayName: 'Log File',
						name: 'logfile',
						type: 'string',
						default: 'uploadLogs',
						description: 'The logfile name, useful for querying',
					},
					{
						displayName: 'Nonce',
						name: 'nonce',
						type: 'string',
						default: '',
						description: 'A unique value to prevent duplicate uploads. Subsequent requests with the same nonce are ignored.',
					},
					{
						displayName: 'Custom Server Fields',
						name: 'customServerFields',
						type: 'fixedCollection',
						typeOptions: {
							multipleValues: true,
						},
						default: {},
						options: [
							{
								name: 'fields',
								displayName: 'Field',
								values: [
									{
										displayName: 'Field Name',
										name: 'name',
										type: 'string',
										default: '',
										placeholder: 'region',
										description: 'Field name (will be prefixed with server-)',
									},
									{
										displayName: 'Field Value',
										name: 'value',
										type: 'string',
										default: '',
										description: 'Field value',
									},
								],
							},
						],
					},
				],
			},

			// ==========================================
			// Add Events Fields
			// ==========================================
			{
				displayName: 'Session ID',
				name: 'sessionId',
				type: 'string',
				required: true,
				default: '',
				placeholder: '={{ $randomUUID }}',
				description: 'A unique string identifying the upload session (up to 200 characters). Use the same session ID for related events.',
				displayOptions: {
					show: {
						resource: ['event'],
						operation: ['addEvents'],
					},
				},
			},
			{
				displayName: 'Events',
				name: 'events',
				type: 'fixedCollection',
				typeOptions: {
					multipleValues: true,
				},
				required: true,
				default: {},
				placeholder: 'Add Event',
				displayOptions: {
					show: {
						resource: ['event'],
						operation: ['addEvents'],
					},
				},
				options: [
					{
						name: 'eventItems',
						displayName: 'Event',
						values: [
							{
								displayName: 'Timestamp',
								name: 'ts',
								type: 'string',
								default: '',
								placeholder: '={{ Date.now() * 1000000 }}',
								description: 'Event timestamp in nanoseconds since 1/1/1970. Leave empty for current time.',
							},
							{
								displayName: 'Message',
								name: 'message',
								type: 'string',
								default: '',
								description: 'The log message text',
							},
							{
								displayName: 'Severity',
								name: 'sev',
								type: 'options',
								options: [
									{ name: '0 - Finest', value: 0 },
									{ name: '1 - Finer/Trace', value: 1 },
									{ name: '2 - Fine/Debug', value: 2 },
									{ name: '3 - Info', value: 3 },
									{ name: '4 - Warning', value: 4 },
									{ name: '5 - Error', value: 5 },
									{ name: '6 - Fatal/Critical', value: 6 },
								],
								default: 3,
								description: 'The severity level of the event',
							},
							{
								displayName: 'Thread ID',
								name: 'thread',
								type: 'string',
								default: '',
								description: 'Thread identifier for the event',
							},
							{
								displayName: 'Additional Attributes (JSON)',
								name: 'additionalAttrs',
								type: 'json',
								default: '{}',
								description: 'Additional key-value attributes for the event as JSON',
							},
						],
					},
				],
			},
			{
				displayName: 'Additional Options',
				name: 'addEventsOptions',
				type: 'collection',
				placeholder: 'Add Option',
				default: {},
				displayOptions: {
					show: {
						resource: ['event'],
						operation: ['addEvents'],
					},
				},
				options: [
					{
						displayName: 'Session Info',
						name: 'sessionInfo',
						type: 'json',
						default: '{}',
						placeholder: '{"serverHost": "front-1", "serverType": "frontend"}',
						description: 'JSON object with session metadata fields like serverHost, serverType, region',
					},
					{
						displayName: 'Threads',
						name: 'threads',
						type: 'json',
						default: '[]',
						placeholder: '[{"id": "1", "name": "request handler thread"}]',
						description: 'JSON array of thread definitions with id and name',
					},
				],
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];

		const resource = this.getNodeParameter('resource', 0) as string;
		const operation = this.getNodeParameter('operation', 0) as string;

		const credentials = await this.getCredentials('sentinelOneDataLakeApi');
		const consoleUrl = (credentials.consoleUrl as string).replace(/\/$/, '');
		const s1Scope = credentials.s1Scope as string;

		for (let i = 0; i < items.length; i++) {
			try {
				let responseData: IDataObject | IDataObject[] | undefined;

				if (resource === 'query') {
					if (operation === 'search') {
						responseData = await executeSearch.call(this, i, consoleUrl, s1Scope) as IDataObject | IDataObject[];
					} else if (operation === 'facet') {
						responseData = await executeFacetQuery.call(this, i, consoleUrl, s1Scope) as IDataObject | IDataObject[];
					} else if (operation === 'timeseries') {
						responseData = await executeTimeseriesQuery.call(this, i, consoleUrl, s1Scope) as IDataObject | IDataObject[];
					} else if (operation === 'powerQuery') {
						responseData = await executePowerQuery.call(this, i, consoleUrl, s1Scope) as IDataObject | IDataObject[];
					}
				} else if (resource === 'event') {
					if (operation === 'uploadLogs') {
						responseData = await executeUploadLogs.call(this, i, consoleUrl, s1Scope) as IDataObject | IDataObject[];
					} else if (operation === 'addEvents') {
						responseData = await executeAddEvents.call(this, i, consoleUrl, s1Scope) as IDataObject | IDataObject[];
					}
				}

				// Handle responses with matches array (search, facet, timeseries, powerQuery)
				// Return each match as a separate item with metadata including continuationToken
				if (responseData && typeof responseData === 'object' && 'matches' in responseData) {
					const { matches, continuationToken, status, sessions, cpuUsage, ...rest } = responseData as IDataObject;
					const metadata = { continuationToken, status, sessions, cpuUsage, ...rest };

					if (Array.isArray(matches) && matches.length > 0) {
						returnData.push(...(matches as IDataObject[]).map((match) => ({
							json: {
								...match,
								_meta: metadata,
							},
						})));
					} else {
						// No matches, return metadata only
						returnData.push({ json: { _meta: metadata } });
					}
				} else if (Array.isArray(responseData)) {
					returnData.push(...responseData.map(item => ({ json: item })));
				} else if (responseData) {
					returnData.push({ json: responseData });
				}
			} catch (error) {
				if (this.continueOnFail()) {
					returnData.push({ json: { error: (error as Error).message }, pairedItem: { item: i } });
					continue;
				}
				throw error;
			}
		}

		return [returnData];
	}
}

async function makeApiRequest(
	this: IExecuteFunctions,
	method: IHttpRequestMethods,
	endpoint: string,
	consoleUrl: string,
	s1Scope: string,
	body?: object,
	headers?: Record<string, string>,
): Promise<unknown> {
	const options: IRequestOptions = {
		method,
		uri: `${consoleUrl}${endpoint}`,
		json: true,
	};

	if (body) {
		options.body = body;
	}

	const additionalHeaders: Record<string, string> = {
		'Content-Type': 'application/json',
		...headers,
	};

	if (s1Scope) {
		additionalHeaders['S1-Scope'] = s1Scope;
	}

	options.headers = additionalHeaders;

	const response = await this.helpers.requestWithAuthentication.call(
		this,
		'sentinelOneDataLakeApi',
		options,
	);

	if (typeof response === 'string') {
		return JSON.parse(response);
	}

	return response;
}

async function executeSearch(
	this: IExecuteFunctions,
	itemIndex: number,
	consoleUrl: string,
	s1Scope: string,
): Promise<unknown> {
	const filter = this.getNodeParameter('filter', itemIndex, '') as string;
	const startTime = this.getNodeParameter('startTime', itemIndex, '24h') as string;
	const endTime = this.getNodeParameter('endTime', itemIndex, '') as string;
	const additionalOptions = this.getNodeParameter('additionalOptions', itemIndex, {}) as {
		maxCount?: number;
		pageMode?: string;
		columns?: string;
		continuationToken?: string;
		priority?: string;
		teamEmails?: string;
		tenant?: boolean;
		accountIds?: string;
	};

	const body: Record<string, unknown> = {
		queryType: 'log',
		filter,
		startTime,
	};

	if (endTime) {
		body.endTime = endTime;
	}

	if (additionalOptions.maxCount) {
		body.maxCount = additionalOptions.maxCount;
	}

	if (additionalOptions.pageMode) {
		body.pageMode = additionalOptions.pageMode;
	}

	if (additionalOptions.columns) {
		body.columns = additionalOptions.columns;
	}

	if (additionalOptions.continuationToken) {
		body.continuationToken = additionalOptions.continuationToken;
	}

	if (additionalOptions.priority) {
		body.priority = additionalOptions.priority;
	}

	if (additionalOptions.teamEmails) {
		body.teamEmails = additionalOptions.teamEmails.split(',').map(e => e.trim());
	}

	if (additionalOptions.tenant !== undefined) {
		body.tenant = additionalOptions.tenant;
	}

	if (additionalOptions.accountIds) {
		body.accountIds = additionalOptions.accountIds.split(',').map(id => id.trim());
	}

	const response = await makeApiRequest.call(this, 'POST', '/api/query', consoleUrl, s1Scope, body);

	return response;
}

async function executeFacetQuery(
	this: IExecuteFunctions,
	itemIndex: number,
	consoleUrl: string,
	s1Scope: string,
): Promise<unknown> {
	const field = this.getNodeParameter('facetField', itemIndex) as string;
	const filter = this.getNodeParameter('facetFilter', itemIndex, '') as string;
	const startTime = this.getNodeParameter('facetStartTime', itemIndex) as string;
	const facetOptions = this.getNodeParameter('facetOptions', itemIndex, {}) as {
		endTime?: string;
		maxCount?: number;
		priority?: string;
	};

	const body: Record<string, unknown> = {
		queryType: 'facet',
		field,
		filter,
		startTime,
	};

	if (facetOptions.endTime) {
		body.endTime = facetOptions.endTime;
	}

	if (facetOptions.maxCount) {
		body.maxCount = facetOptions.maxCount;
	}

	if (facetOptions.priority) {
		body.priority = facetOptions.priority;
	}

	const response = await makeApiRequest.call(this, 'POST', '/api/facetQuery', consoleUrl, s1Scope, body);

	return response;
}

async function executeTimeseriesQuery(
	this: IExecuteFunctions,
	itemIndex: number,
	consoleUrl: string,
	s1Scope: string,
): Promise<unknown> {
	const timeseriesQueries = this.getNodeParameter('timeseriesQueries', itemIndex, {}) as {
		queryItems?: Array<{
			filter: string;
			startTime: string;
			endTime?: string;
			function?: string;
			buckets?: number;
			createSummaries?: boolean;
			onlyUseSummaries?: boolean;
			priority?: string;
		}>;
	};

	const queries = (timeseriesQueries.queryItems || []).map(q => {
		const query: Record<string, unknown> = {
			filter: q.filter || '',
			startTime: q.startTime,
		};

		if (q.endTime) {
			query.endTime = q.endTime;
		}

		if (q.function) {
			query.function = q.function;
		}

		if (q.buckets) {
			query.buckets = q.buckets;
		}

		if (q.createSummaries !== undefined) {
			query.createSummaries = q.createSummaries;
		}

		if (q.onlyUseSummaries !== undefined) {
			query.onlyUseSummaries = q.onlyUseSummaries;
		}

		if (q.priority) {
			query.priority = q.priority;
		}

		return query;
	});

	if (queries.length === 0) {
		throw new NodeOperationError(this.getNode(), 'At least one query is required for timeseries query');
	}

	const body = { queries };

	const response = await makeApiRequest.call(this, 'POST', '/api/timeseriesQuery', consoleUrl, s1Scope, body);

	return response;
}

async function executePowerQuery(
	this: IExecuteFunctions,
	itemIndex: number,
	consoleUrl: string,
	s1Scope: string,
): Promise<unknown> {
	const query = this.getNodeParameter('powerQueryExpression', itemIndex) as string;
	const startTime = this.getNodeParameter('powerQueryStartTime', itemIndex, '24h') as string;
	const powerQueryOptions = this.getNodeParameter('powerQueryOptions', itemIndex, {}) as {
		endTime?: string;
		priority?: string;
		teamEmails?: string;
		tenant?: boolean;
		accountIds?: string;
	};

	const body: Record<string, unknown> = {
		query,
		startTime,
	};

	if (powerQueryOptions.endTime) {
		body.endTime = powerQueryOptions.endTime;
	}

	if (powerQueryOptions.priority) {
		body.priority = powerQueryOptions.priority;
	}

	if (powerQueryOptions.teamEmails) {
		body.teamEmails = powerQueryOptions.teamEmails.split(',').map(e => e.trim());
	}

	if (powerQueryOptions.tenant !== undefined) {
		body.tenant = powerQueryOptions.tenant;
	}

	if (powerQueryOptions.accountIds) {
		body.accountIds = powerQueryOptions.accountIds.split(',').map(id => id.trim());
	}

	const response = await makeApiRequest.call(this, 'POST', '/api/powerQuery', consoleUrl, s1Scope, body);

	return response;
}

async function executeUploadLogs(
	this: IExecuteFunctions,
	itemIndex: number,
	consoleUrl: string,
	s1Scope: string,
): Promise<unknown> {
	const logData = this.getNodeParameter('logData', itemIndex) as string;
	const uploadLogsOptions = this.getNodeParameter('uploadLogsOptions', itemIndex, {}) as {
		parser?: string;
		serverHost?: string;
		logfile?: string;
		nonce?: string;
		customServerFields?: {
			fields?: Array<{
				name: string;
				value: string;
			}>;
		};
	};

	const headers: Record<string, string> = {
		'Content-Type': 'text/plain',
	};

	if (uploadLogsOptions.parser) {
		headers['parser'] = uploadLogsOptions.parser;
	}

	if (uploadLogsOptions.serverHost) {
		headers['server-host'] = uploadLogsOptions.serverHost;
	}

	if (uploadLogsOptions.logfile) {
		headers['logfile'] = uploadLogsOptions.logfile;
	}

	if (uploadLogsOptions.nonce) {
		headers['Nonce'] = uploadLogsOptions.nonce;
	}

	if (uploadLogsOptions.customServerFields?.fields) {
		for (const field of uploadLogsOptions.customServerFields.fields) {
			if (field.name && field.value) {
				headers[`server-${field.name}`] = field.value;
			}
		}
	}

	if (s1Scope) {
		headers['S1-Scope'] = s1Scope;
	}

	const credentials = await this.getCredentials('sentinelOneDataLakeApi');

	const options: IRequestOptions = {
		method: 'POST',
		uri: `${consoleUrl}/api/uploadLogs`,
		body: logData,
		headers: {
			...headers,
			Authorization: `Bearer ${credentials.apiToken}`,
		},
		json: false,
	};

	const response = await this.helpers.request(options);

	return typeof response === 'string' ? JSON.parse(response) : response;
}

async function executeAddEvents(
	this: IExecuteFunctions,
	itemIndex: number,
	consoleUrl: string,
	s1Scope: string,
): Promise<unknown> {
	const sessionId = this.getNodeParameter('sessionId', itemIndex) as string;
	const eventsData = this.getNodeParameter('events', itemIndex, {}) as {
		eventItems?: Array<{
			ts?: string;
			message?: string;
			sev?: number;
			thread?: string;
			additionalAttrs?: string | object;
		}>;
	};
	const addEventsOptions = this.getNodeParameter('addEventsOptions', itemIndex, {}) as {
		sessionInfo?: string | object;
		threads?: string | object;
	};

	const events = (eventsData.eventItems || []).map(event => {
		const attrs: Record<string, unknown> = {};

		if (event.message) {
			attrs.message = event.message;
		}

		// Parse additional attributes
		if (event.additionalAttrs) {
			const additionalAttrs = typeof event.additionalAttrs === 'string'
				? JSON.parse(event.additionalAttrs)
				: event.additionalAttrs;
			Object.assign(attrs, additionalAttrs);
		}

		const eventObj: Record<string, unknown> = {
			attrs,
		};

		if (event.ts) {
			eventObj.ts = event.ts;
		} else {
			// Default to current time in nanoseconds
			eventObj.ts = String(Date.now() * 1000000);
		}

		if (event.sev !== undefined) {
			eventObj.sev = event.sev;
		}

		if (event.thread) {
			eventObj.thread = event.thread;
		}

		return eventObj;
	});

	const body: Record<string, unknown> = {
		session: sessionId,
		events,
	};

	if (addEventsOptions.sessionInfo) {
		body.sessionInfo = typeof addEventsOptions.sessionInfo === 'string'
			? JSON.parse(addEventsOptions.sessionInfo)
			: addEventsOptions.sessionInfo;
	}

	if (addEventsOptions.threads) {
		body.threads = typeof addEventsOptions.threads === 'string'
			? JSON.parse(addEventsOptions.threads)
			: addEventsOptions.threads;
	}

	const response = await makeApiRequest.call(this, 'POST', '/api/addEvents', consoleUrl, s1Scope, body);

	return response;
}
