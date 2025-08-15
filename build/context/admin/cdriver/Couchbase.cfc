component extends="Cache" {
	variables.fields = [
		field(displayName = "Connection String",
			name = "connectionString",
			defaultValue = "",
			required = true,
			description = "connection string to connect to Couchbase, use this pattern to connect to a Couchbase Capella instance [couchbases://cb.{your-endpoint-here}.cloud.couchbase.com] 
			and the following pattern for a regular couchbase server [couchbase://{your-endpoint-here}]. ",
			type = "text"
			)

		,group("Authentication","Authentication Credentials")
		,field(displayName = "Username",
			name = "username",
			defaultValue = "",
			required = true,
			description = "Username necessary to connect.",
			type = "text"
		)
		,field(displayName = "Password",
			name = "password",
			defaultValue = "",
			required = true,
			description = "Password necessary to connect.",
			type = "password"
		)

		,group("Default Endpoint","Location within the cache")
		,field(displayName = "Default Bucket Name",
			name = "bucketName",
			defaultValue = "",
			required = false,
			description = "name of the bucket within the cache to use. if not defined, you have to define the bucket as part of the key in the code like this [{bucket}/{scope}/{collection}/{key}].",
			type = "text"
		)
		,field(displayName = "Default Scope Name",
			name = "scopeName",
			defaultValue = "",
			required = false,
			description = "name of the scope within the bucket (defined above) to use. if not defined, you have to define the scope as part of the key in the code like this [{scope}/{collection}/{key}].
				If you define a scope you also have to define a bucket (above).",
			type = "text"
		)
		,field(displayName = "Default Collection Name",
			name = "collectionName",
			defaultValue = "",
			required = false,
			description = "name of the collection within the scope (defined above) to use. if not defined, you have to define the collection as part of the key in the code like this [{collection}/{key}].
				If you define a collection you also have to define a scope (above).",
			type = "text"
		)
		,field(displayName = "Create Bucket if necessary",
			name = "createIfNecessaryBucket",
			defaultValue = false,
			required = false,
			description = "If set to true and the requested bucket does not, it get created automatically, if set to false it will throw an exception that the bucket does not exist.",
			type = "checkbox",
			values = true
		)
		,field(displayName = "Create Scope if necessary",
			name = "createIfNecessaryScope",
			defaultValue = false,
			required = false,
			description = "If set to true and the requested scope does not, it get created automatically, if set to false it will throw an exception that the scope does not exist.",
			type = "checkbox",
			values = true
		)
		,field(displayName = "Create Collection if necessary",
			name = "createIfNecessaryCollection",
			defaultValue = false,
			required = false,
			description = "If set to true and the requested collection does not, it get created automatically, if set to false it will throw an exception that the collection does not exist.",
			type = "checkbox",
			values = true
		)

		,group("General","General settings to customize the cache behaviour")
		,field(displayName = "Transcoder",
			name = "transcoder",
			defaultValue = "Object,JSON",
			required = true,
			description = "How does the data get transcoded/encoded/decoded. If you choose ""Object"" you can store all kind of data in the cache, if you choose ""JSON"" you can only store json compatible structures (Struct or Java Map). ""JSON"" should only be used, if you plan to access the cache also outside Lucee, because ""JSON"" is the default structure for Couchbase.",
			type = "radio"
		)
		,field(displayName = "Default expire time (ms)",
			name = "defaultExpire",
			defaultValue = "0",
			required = false,
			description = "Expire time for a Document (Cache Entry) in milliseconds, 0 is equal to never.",
			type = "text"
		)
		,field(displayName = "Connection timeout (ms)",
			name = "connectionTimeout",
			defaultValue = "10000",
			required = false,
			description = "Connection timeout in milliseconds.",
			type = "text"
		)

	];

	public string function getClass() {
		return "{class}";
	}

	public string function getLabel() {
		return "{label}";
	}

	public string function getDescription() {
		return "{desc}";
	}
}
