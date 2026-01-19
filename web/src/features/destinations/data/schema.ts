import { z } from 'zod'

export const destinationSchema = z.object({
    id: z.number(),
    name: z.string(),
    snowflake_account: z.string().nullable(),
    snowflake_user: z.string().nullable(),
    snowflake_database: z.string().nullable(),
    snowflake_schema: z.string().nullable(),
    snowflake_landing_database: z.string().nullable(),
    snowflake_landing_schema: z.string().nullable(),
    snowflake_role: z.string().nullable(),
    snowflake_private_key: z.string().nullable(),
    // snowflake_private_key_passphrase: z.string().nullable(), // Usually we don't return this or show it
    snowflake_warehouse: z.string().nullable(),
    created_at: z.string(),
    updated_at: z.string(),
})

export type Destination = z.infer<typeof destinationSchema>

export const destinationFormSchema = z.object({
    name: z.string().min(1, 'Name is required').regex(/^\S*$/, 'Name must not contain whitespace'),
    snowflake_account: z.string().optional(),
    snowflake_user: z.string().optional(),
    snowflake_database: z.string().optional(),
    snowflake_schema: z.string().optional(),
    snowflake_landing_database: z.string().optional(),
    snowflake_landing_schema: z.string().optional(),
    snowflake_role: z.string().optional(),
    // Using string for the key content which will be read from file
    snowflake_private_key: z.string().optional(),
    snowflake_private_key_passphrase: z.string().optional(),
    snowflake_warehouse: z.string().optional(),
})

export type DestinationForm = z.infer<typeof destinationFormSchema>
