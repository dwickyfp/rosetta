
import "ace-builds/src-noconflict/ext-language_tools";

/**
 * Creates a custom SQL completer that handles dynamic aliases and destination tables.
 * 
 * @param schema - Schema for source tables (table_name -> columns)
 * @param fetchDestinationSchema - Async function to fetch schema for a specific destination table
 * @param destinationName - Name of the destination (used for pg_<dest_name> prefix)
 * @param onLoading - Callback to indicate loading state (true/false)
 */
export const createSqlCompleter = (
    schema: Record<string, string[]>,
    fetchDestinationSchema: (tableName: string) => Promise<string[]>,
    destinationName?: string,
    onLoading?: (isLoading: boolean) => void
) => {
    // Local cache for destination tables to avoid repeated API calls
    const destinationCache: Record<string, string[]> = {};

    return {
        getCompletions: async (editor: any, session: any, pos: any, prefix: any, callback: any) => {
            const line = session.getLine(pos.row);
            const textBeforeCursor = line.substring(0, pos.column);

            const dotMatch = textBeforeCursor.match(/([a-zA-Z0-9_\-]+(?:\.[a-zA-Z0-9_\-]+)*)\.\s*$/);

            if (dotMatch) {
                const identifierChain = dotMatch[1].toLowerCase();
                const fullSql = editor.getValue();

                // 1. Parse Aliases with awareness of SQL structure
                const aliasMap: Record<string, string> = {};

                // Remove comments to avoid false positives
                const cleanSql = fullSql.replace(/--.*$/gm, "").replace(/\/\*[\s\S]*?\*\//g, "");

                // Keywords that usually precede a table reference
                // Simplified and more robust regex
                const tableRefRegex = /\b(FROM|JOIN|UPDATE|INTO|USING)\s+([a-zA-Z0-9_.\-]+)(?:\s+(?:AS\s+)?([a-zA-Z0-9_]+))?\b/gi;

                let m;
                while ((m = tableRefRegex.exec(cleanSql)) !== null) {
                    const tableName = m[2].toLowerCase();
                    const aliasName = m[3] ? m[3].toLowerCase() : null;

                    if (aliasName) {
                        // Check if alias is a keyword
                        const isKeyword = ["select", "from", "join", "where", "as", "on", "and", "or", "group", "order", "limit", "left", "right", "inner", "outer", "using", "update", "set", "insert", "into"].includes(aliasName);
                        if (!isKeyword) {
                            aliasMap[aliasName] = tableName;
                        }
                    }
                }
                const destPrefix = destinationName ? `pg_${destinationName.toLowerCase()}` : 'pg_dest';

                if (identifierChain === destPrefix) {
                    let tables: string[] = [];
                    if (destinationCache['__tables__']) {
                        tables = destinationCache['__tables__'];
                    } else {
                        try {
                            if (onLoading) onLoading(true);
                            tables = await fetchDestinationSchema("");
                            if (tables.length > 0) {
                                destinationCache['__tables__'] = tables;
                            }
                        } catch (e) {
                        } finally {
                            if (onLoading) onLoading(false);
                        }
                    }

                    return callback(null, tables.map(t => ({
                        caption: t,
                        value: t,
                        meta: "dest table",
                        score: 2000
                    })));
                }

                if (identifierChain.startsWith(destPrefix + ".")) {
                    const tableName = identifierChain.replace(destPrefix + ".", "");
                    let columns: string[] = [];

                    if (destinationCache[tableName]) {
                        columns = destinationCache[tableName];
                    } else {
                        try {
                            if (onLoading) onLoading(true);
                            columns = await fetchDestinationSchema(tableName);
                            if (columns.length > 0) {
                                destinationCache[tableName] = columns;
                            }
                        } catch (e) {
                            console.error("Fetch error:", e);
                        } finally {
                            if (onLoading) onLoading(false);
                        }
                    }

                    return callback(null, columns.map(c => ({
                        caption: c,
                        value: c,
                        meta: "dest column",
                        score: 1000
                    })));
                }

                if (aliasMap[identifierChain]) {
                    const realTableName = aliasMap[identifierChain];

                    if (schema[realTableName]) {
                        return callback(null, schema[realTableName].map(c => ({
                            caption: c,
                            value: c,
                            meta: "source column",
                            score: 1000
                        })));
                    }

                    // Is it a destination table alias? 
                    // e.g. realTableName = "pg_pg_target_2.tbl_sales_region"
                    if (realTableName.startsWith(destPrefix + ".")) {
                        const tableName = realTableName.replace(destPrefix + ".", "");
                        let columns: string[] = [];

                        if (destinationCache[tableName]) {
                            columns = destinationCache[tableName];
                        } else {
                            try {
                                if (onLoading) onLoading(true);
                                columns = await fetchDestinationSchema(tableName);
                                if (columns.length > 0) {
                                    destinationCache[tableName] = columns;
                                }
                            } catch (e) {
                                console.error("Fetch error:", e);
                            } finally {
                                if (onLoading) onLoading(false);
                            }
                        }

                        return callback(null, columns.map(c => ({
                            caption: c,
                            value: c,
                            meta: "dest column",
                            score: 1000
                        })));
                    }
                }

                if (schema[identifierChain]) {
                    return callback(null, schema[identifierChain].map(c => ({
                        caption: c,
                        value: c,
                        meta: "source column",
                        score: 1000
                    })));
                }
            }

            // 3. Default Suggestions (Keywords only, cleaner)
            // Since we rely on standard keyword completer, we return nothing here unless we want to suggest table names at top level
            if (prefix.length >= 0) {
                // Only suggest source table names and destination prefix at basic level
                const suggestions = [];
                // Source Tables
                suggestions.push(...Object.keys(schema).map(table => ({
                    caption: table,
                    value: table,
                    meta: "source table",
                    score: 500
                })));

                // Destination Prefix Hint
                if (destinationName) {
                    const destPrefix = `pg_${destinationName.toLowerCase()}`;
                    suggestions.push({
                        caption: destPrefix,
                        value: destPrefix,
                        meta: "destination schema",
                        score: 600
                    });
                }
                callback(null, suggestions);
            }
        }
    };
};
