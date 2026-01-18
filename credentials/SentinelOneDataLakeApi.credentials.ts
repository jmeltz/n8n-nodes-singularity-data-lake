import {
	IAuthenticateGeneric,
	ICredentialTestRequest,
	ICredentialType,
	INodeProperties,
} from 'n8n-workflow';

export class SentinelOneDataLakeApi implements ICredentialType {
	name = 'sentinelOneDataLakeApi';
	displayName = 'SentinelOne Singularity Data Lake API';
	documentationUrl = 'https://docs.sentinelone.com/';
	properties: INodeProperties[] = [
		{
			displayName: 'Console URL',
			name: 'consoleUrl',
			type: 'string',
			default: '',
			placeholder: 'https://xdr.us1.sentinelone.net',
			description: 'The URL for your Singularity Data Lake Console (e.g., https://xdr.us1.sentinelone.net)',
			required: true,
		},
		{
			displayName: 'API Token',
			name: 'apiToken',
			type: 'string',
			typeOptions: {
				password: true,
			},
			default: '',
			description: 'Your SDL API Key (Log Read Access, Log Write Access, etc.) or SentinelOne Console API Token',
			required: true,
		},
		{
			displayName: 'S1-Scope (Optional)',
			name: 's1Scope',
			type: 'string',
			default: '',
			placeholder: 'accountId:siteId or accountId',
			description: 'Required for multi-Account/Site users. Format: "accountId:siteId" for Site scope or "accountId" for Account scope.',
		},
	];

	authenticate: IAuthenticateGeneric = {
		type: 'generic',
		properties: {
			headers: {
				Authorization: '=Bearer {{$credentials.apiToken}}',
			},
		},
	};

	test: ICredentialTestRequest = {
		request: {
			baseURL: '={{$credentials.consoleUrl}}',
			url: '/api/query',
			method: 'POST',
			headers: {
				'Content-Type': 'application/json',
			},
			body: JSON.stringify({
				queryType: 'log',
				maxCount: 1,
				startTime: '1h',
			}),
		},
	};
}
