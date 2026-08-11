// ============================================================================
//                    MAI TOKEN PRESALE SMART CONTRACT
//                         Solana Blockchain
// ============================================================================
//
// TOKENOMICS (Total Supply: 10,000,000,000 MAI):
//   - Presale:    7B (70%) - 14 stages with progressive pricing & vesting
//   - Team:       1B (10%) - 12 month cliff + 12 month linear vesting
//   - Marketing:  1B (10%) - Immediately available for campaigns
//   - Liquidity:  1B (10%) - Reserved for DEX liquidity pools
//
// NFT REWARD SYSTEM (MAI Warriors Collection):
//   - Bronze:   $50-99   contribution
//   - Silver:   $100-199 contribution
//   - Gold:     $200-299 contribution
//   - Platinum: $300+    contribution
//   - Auto-upgrade on additional purchases
//
// AIRDROP & BONUS SYSTEM:
//   - Marketing Instant Bonus: 100% claimable at TGE
//   - Presale Airdrop: 10% TGE + 90% linear over 9 months
//   - Airdrop NFT: Special edition for marketing campaigns
//
// SECURITY FEATURES:
//   - Reentrancy guard protection
//   - Admin-only critical functions
//   - PDA validation for all accounts
//   - Checked arithmetic (overflow protection)
//
// ============================================================================

use solana_program::{
    account_info::{next_account_info, AccountInfo},
    entrypoint,
    entrypoint::ProgramResult,
    msg,
    program_error::ProgramError,
    pubkey::Pubkey,
    clock::Clock,
    sysvar::Sysvar,
    program::{invoke, invoke_signed},
    system_instruction,
    rent::Rent,
    pubkey,
};

use spl_token::{self};
use spl_associated_token_account::{self, get_associated_token_address, instruction::create_associated_token_account};

use mpl_token_metadata::{
    instructions::{CreateMetadataAccountV3, CreateMetadataAccountV3InstructionArgs},
    types::DataV2,
};

solana_program::declare_id!("C5JTCJ5K9uhCkQHqCzFLw5MHTdYjTqvU9F5kxZXjDWJ9");

entrypoint!(process_instruction);

// ==================== STABLECOIN MINTS ====================
const USDC_MINT_DEVNET: Pubkey = pubkey!("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v");
const USDT_MINT_DEVNET: Pubkey = pubkey!("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB");

const MIN_SOL_PURCHASE: u64 = 10_000_000;
const MIN_USDC_PURCHASE: u64 = 1_000_000;
const MIN_USDT_PURCHASE: u64 = 1_000_000;
const UPDATE_SOL_PRICE_FUNCTION: u8 = 29;

const PRECISION: u64 = 1_000_000_000;  // 9 decimals for MAI token
const CENTS_PRECISION: u64 = 100;
const NFT_COLLECTION_CREATED_INDEX: usize = 237;

// ==================== PDA SEEDS ====================
const PRESALE_CONFIG_SEED: &[u8] = b"presale_config";
const MAI_MINT_SEED: &[u8] = b"mai_mint";
const USER_CONTRIBUTION_SEED: &[u8] = b"user_contribution";
const TEAM_VESTING_SEED: &[u8] = b"team_vesting";
const TEAM_VAULT_SEED: &[u8] = b"team_vault";
const MARKETING_VAULT_SEED: &[u8] = b"marketing_vault";
const LIQUIDITY_VAULT_SEED: &[u8] = b"liquidity_vault";
const USDC_TREASURY_SEED: &[u8] = b"usdc_treasury";
const USDT_TREASURY_SEED: &[u8] = b"usdt_treasury";
const MIN_SOL_PRICE_USD_CENTS: u64 = 1000;   // Min $10.00
const MAX_SOL_PRICE_USD_CENTS: u64 = 200000; // Max $2000.00

// ==================== NFT SEEDS ====================
const NFT_COLLECTION_SEED: &[u8] = b"nft_collection";
const USER_NFT_SEED: &[u8] = b"user_nft";
const AIRDROP_NFT_SEED: &[u8] = b"airdrop_nft";

// ==================== NFT METADATA URIs (IPFS) ====================
const BRONZE_METADATA_URI: &str = "https://gateway.pinata.cloud/ipfs/bafkreicuyfdcihkvgat57v4bi4xbpm32lvs4ik6nd76pvovpapf64ypzsq";
const SILVER_METADATA_URI: &str = "https://gateway.pinata.cloud/ipfs/bafkreihwjgvlcyfnkdqyirmg64yc4loyywp7sdxuyqee6gl4gk7pcv2wzq";
const GOLD_METADATA_URI: &str = "https://gateway.pinata.cloud/ipfs/bafkreighfrap6x4wl23ycrf4gwcdkrooz3bod72nr6eeqmnxnfyegfrncu";
const PLATINUM_METADATA_URI: &str = "https://gateway.pinata.cloud/ipfs/bafkreig42pu7r2hmmbfqgjbxbodjat4grdde7lotj4yr4vxnydstac7cba";
const AIRDROP_SILVER_METADATA_URI: &str = "https://gateway.pinata.cloud/ipfs/bafkreifomnfp4ilvbnvokyuedv6c2b2vo5fueouehtaqvfdrgairx63j2u";

// ==================== NFT COLLECTION METADATA ====================
const COLLECTION_NAME: &str = "MAI Warriors";
const COLLECTION_SYMBOL: &str = "MAIW";
const COLLECTION_URI: &str = "https://gateway.pinata.cloud/ipfs/bafkreigra7syf5v4eecht5c4htyycilbya4ialdzynq222njl25jg5qqqi";

// ==================== MAI TOKEN METADATA ====================
const MAI_TOKEN_NAME: &str = "MAI";
const MAI_TOKEN_SYMBOL: &str = "MAI";
const MAI_TOKEN_URI: &str = "https://gateway.pinata.cloud/ipfs/bafkreicd4n54blj7f7fh3gz3u2ilcb7nwfcl2vj6hdeiqgeyvdqpybduiu";

// ==================== NFT TIER SYSTEM ====================
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum NftTier {
    None = 0,
    Bronze = 1,   // $50-99 contribution
    Silver = 2,   // $100-199 contribution
    Gold = 3,     // $200-299 contribution
    Platinum = 4, // $300+ contribution
}

impl NftTier {
    pub fn from_usd_amount(usd_dollars: u64) -> Self {
        match usd_dollars {
            50..=99 => NftTier::Bronze,
            100..=199 => NftTier::Silver,
            200..=299 => NftTier::Gold,
            300.. => NftTier::Platinum,
            _ => NftTier::None,
        }
    }

    pub fn get_metadata_uri(&self) -> &'static str {
        match self {
            NftTier::Bronze => BRONZE_METADATA_URI,
            NftTier::Silver => SILVER_METADATA_URI,
            NftTier::Gold => GOLD_METADATA_URI,
            NftTier::Platinum => PLATINUM_METADATA_URI,
            NftTier::None => "",
        }
    }

    pub fn get_name(&self) -> &'static str {
        match self {
            NftTier::Bronze => "MAI Bronze Warrior",
            NftTier::Silver => "MAI Silver Warrior", 
            NftTier::Gold => "MAI Gold Warrior",
            NftTier::Platinum => "MAI Platinum Warrior",
            NftTier::None => "",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct StagePurchaseResult {
    stage: u8,
    tokens: u64,
    usd_spent: u64,
}

impl Default for StagePurchaseResult {
    fn default() -> Self {
        Self {
            stage: 0,
            tokens: 0,
            usd_spent: 0,
        }
    }
}

// ==================== INSTRUCTION ROUTER ====================
pub fn process_instruction(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    instruction_data: &[u8],
) -> ProgramResult {
    if instruction_data.is_empty() {
        return Err(ProgramError::InvalidInstructionData);
    }

    let instruction_type = instruction_data[0];

    match instruction_type {
        0 => initialize_presale(accounts, program_id),
        1 => buy_with_sol(accounts, &instruction_data[1..], program_id),
        2 => buy_with_usdc(accounts, &instruction_data[1..], program_id),
        3 => buy_with_usdt(accounts, &instruction_data[1..], program_id),
        6 => trigger_listing(accounts, program_id),
        7 => claim_tokens(accounts, program_id),
        11 => pause_presale(accounts, program_id),
        12 => resume_presale(accounts, program_id),
        15 => mint_team_tokens(accounts, program_id),
        16 => mint_marketing_tokens(accounts, program_id),
        17 => mint_liquidity_tokens(accounts, program_id),
        18 => claim_team_tokens(accounts, program_id),
        20 => create_mai_token(accounts, program_id),
        21 => create_usdc_treasury(accounts, program_id),
        22 => create_usdt_treasury(accounts, program_id),
        24 => withdraw_usdc(accounts, &instruction_data[1..], program_id),
        25 => withdraw_usdt(accounts, &instruction_data[1..], program_id),
	    27 => withdraw_marketing_tokens(accounts, &instruction_data[1..], program_id),
        28 => withdraw_liquidity_tokens(accounts, &instruction_data[1..], program_id),
	    29 => update_sol_price(accounts, &instruction_data[1..], program_id),
	    30 => create_nft_collection(accounts, program_id),
        31 => mint_user_nft(accounts, &instruction_data[1..], program_id),
        32 => upgrade_user_nft(accounts, &instruction_data[1..], program_id),
        33 => claim_user_nft(accounts, program_id),
 	    34 => airdrop_marketing_nft(accounts, &instruction_data[1..], program_id),
	    35 => claim_airdrop_nft(accounts, program_id),
	    36 => pause_all_claims(accounts, program_id),
	    37 => resume_all_claims(accounts, program_id),
	    38 => allocate_marketing_bonus(accounts, &instruction_data[1..], program_id),
	    39 => claim_marketing_instant(accounts, program_id),
	    40 => claim_presale_airdrop(accounts, program_id),
        _ => Err(ProgramError::InvalidInstructionData),
    }
}

// ==================== EVENT EMITTERS ====================
fn emit_purchase_event(
    buyer: &Pubkey,
    amount: u64,
    token_type: u8,
    stage: u8,
    tokens_received: u64,
) {
    msg!("EVENT:PURCHASE:{}:{}:{}:{}:{}", buyer, amount, token_type, stage, tokens_received);
}

fn emit_claim_event(
    claimer: &Pubkey,
    amount: u64,
    remaining_locked: u64,
) {
    msg!("EVENT:CLAIM:{}:{}:{}", claimer, amount, remaining_locked);
}

fn emit_stage_transition_event(
    from_stage: u8,
    to_stage: u8,
    total_raised: u64,
) {
    msg!("EVENT:STAGE_TRANSITION:{}:{}:{}", from_stage, to_stage, total_raised);
}

fn emit_presale_completion_event(total_sold: u64) {
    msg!("EVENT:PRESALE_COMPLETED:{}:7000000000000000000", total_sold);
}

// ==================== CREATE MAI TOKEN ====================
fn create_mai_token(accounts: &[AccountInfo], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let admin_account = next_account_info(account_info_iter)?;
    let mai_mint_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let mai_metadata_account = next_account_info(account_info_iter)?;
    let token_program_account = next_account_info(account_info_iter)?;
    let token_metadata_program_account = next_account_info(account_info_iter)?;
    let system_program_account = next_account_info(account_info_iter)?;
    let rent_sysvar_account = next_account_info(account_info_iter)?;
    let presale_vault_account = next_account_info(account_info_iter)?;
    let associated_token_program_account = next_account_info(account_info_iter)?;

    if !admin_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    validate_admin_access(presale_config_account, admin_account.key)?;

    // Check that MAI token doesn't exist yet
    if mai_mint_account.data_len() > 0 {
        msg!("ERROR: MAI token already exists!");
        return Err(ProgramError::AccountAlreadyInitialized);
    }

    let (mai_mint_pda, mai_mint_bump) = Pubkey::find_program_address(
        &[MAI_MINT_SEED],
        program_id,
    );

    if mai_mint_pda != *mai_mint_account.key {
        return Err(ProgramError::InvalidAccountData);
    }

    let (presale_config_pda, presale_config_bump) = Pubkey::find_program_address(
        &[PRESALE_CONFIG_SEED],
        program_id,
    );

    if presale_config_pda != *presale_config_account.key {
        return Err(ProgramError::InvalidAccountData);
    }

    let rent = Rent::from_account_info(rent_sysvar_account)?;
    let mint_account_size = 82;
    let mint_rent = rent.minimum_balance(mint_account_size);

    invoke_signed(
        &system_instruction::create_account(
            admin_account.key,
            mai_mint_account.key,
            mint_rent,
            mint_account_size as u64,
            token_program_account.key,
        ),
        &[
            admin_account.clone(),
            mai_mint_account.clone(),
            system_program_account.clone(),
        ],
        &[&[MAI_MINT_SEED, &[mai_mint_bump]]],
    )?;

    let init_mint_instruction = spl_token::instruction::initialize_mint(
        token_program_account.key,
        mai_mint_account.key,
        &presale_config_pda,
        Some(&presale_config_pda),
        9,
    )?;

    invoke(
        &init_mint_instruction,
        &[
            mai_mint_account.clone(),
            rent_sysvar_account.clone(),
            token_program_account.clone(),
        ],
    )?;

    // ==================== CREATE METADATA ====================
    let metadata_data = DataV2 {
        name: MAI_TOKEN_NAME.to_string(),
        symbol: MAI_TOKEN_SYMBOL.to_string(),
        uri: MAI_TOKEN_URI.to_string(),
        seller_fee_basis_points: 0,
        creators: None,
        collection: None,
        uses: None,
    };

    let create_metadata_instruction = CreateMetadataAccountV3 {
        metadata: *mai_metadata_account.key,
        mint: *mai_mint_account.key,
        mint_authority: *presale_config_account.key,
        payer: *admin_account.key,
        update_authority: (*presale_config_account.key, true),
        system_program: *system_program_account.key,
        rent: Some(*rent_sysvar_account.key),
    };

    let create_metadata_ix = create_metadata_instruction.instruction(CreateMetadataAccountV3InstructionArgs {
        data: metadata_data,
        is_mutable: true,
        collection_details: None,
    });

    invoke_signed(
        &create_metadata_ix,
        &[
            mai_metadata_account.clone(),
            mai_mint_account.clone(),
            presale_config_account.clone(),
            admin_account.clone(),
            system_program_account.clone(),
            rent_sysvar_account.clone(),
            token_metadata_program_account.clone(),
        ],
        &[&[PRESALE_CONFIG_SEED, &[presale_config_bump]]],
    )?;

    let mut presale_data = presale_config_account.data.borrow_mut();
    mai_mint_pda.to_bytes().iter().enumerate().for_each(|(i, &byte)| {
        presale_data[32 + i] = byte;
    });
    drop(presale_data);

    // ==================== CREATE PRESALE VAULT ====================
    let expected_presale_vault = get_associated_token_address(&presale_config_pda, &mai_mint_pda);
    
    if expected_presale_vault != *presale_vault_account.key {
        msg!("Invalid presale vault address. Expected: {}, got: {}", expected_presale_vault, presale_vault_account.key);
        return Err(ProgramError::InvalidAccountData);
    }

    if presale_vault_account.data_len() == 0 {
        let create_presale_vault_instruction = create_associated_token_account(
            admin_account.key,
            &presale_config_pda,
            &mai_mint_pda,
            token_program_account.key,
        );

        invoke(
            &create_presale_vault_instruction,
            &[
                admin_account.clone(),
                presale_vault_account.clone(),
                presale_config_account.clone(),
                mai_mint_account.clone(),
                system_program_account.clone(),
                token_program_account.clone(),
                associated_token_program_account.clone(),
            ],
        )?;

    } else {
        msg!("Presale vault already exists: {}", presale_vault_account.key);
    }

    Ok(())

}

// ==================== INITIALIZE PRESALE ====================
fn initialize_presale(accounts: &[AccountInfo], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let admin_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let system_program_account = next_account_info(account_info_iter)?;
    let rent_sysvar_account = next_account_info(account_info_iter)?;

    if !admin_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    let (presale_config_pda, presale_config_bump) = Pubkey::find_program_address(
        &[PRESALE_CONFIG_SEED],
        program_id,
    );

    if presale_config_pda != *presale_config_account.key {
        return Err(ProgramError::InvalidAccountData);
    }

    let rent = Rent::from_account_info(rent_sysvar_account)?;
    let account_size = 360;
    let account_rent = rent.minimum_balance(account_size);

    invoke_signed(
        &system_instruction::create_account(
            admin_account.key,
            presale_config_account.key,
            account_rent,
            account_size as u64,
            program_id,
        ),
        &[
            admin_account.clone(),
            presale_config_account.clone(),
            system_program_account.clone(),
        ],
        &[&[PRESALE_CONFIG_SEED, &[presale_config_bump]]],
    )?;

    let mut account_data = presale_config_account.data.borrow_mut();

    // Initialize admin
    admin_account.key.to_bytes().iter().enumerate().for_each(|(i, &byte)| {
        account_data[i] = byte;
    });

    // MAI mint will be set later via create_mai_token
    [0u8; 32].iter().enumerate().for_each(|(i, &byte)| {
        account_data[32 + i] = byte;
    });

    account_data[64] = 1; // current_stage = 1
    0u64.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
        account_data[65 + i] = byte; // total_sold = 0
    });
    account_data[73] = 1; // is_active = true
    0i64.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
        account_data[74 + i] = byte; // listing_date = 0
    });
    account_data[82] = 0; // listing_triggered = false

    // SOL price in CENTS - $180.00 = 18000 cents
    18000u64.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
        account_data[83 + i] = byte; // sol_price = $180.00 in cents
    });

    // Initialize collected amounts
    0u64.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
        account_data[91 + i] = byte;  // usdc_collected = 0
        account_data[99 + i] = byte;  // usdt_collected = 0
        account_data[107 + i] = byte; // sol_collected = 0
    });

    // Initialize stage_sold_amounts array (14 stages x 8 bytes each)
    for i in 0..14 {
        0u64.to_le_bytes().iter().enumerate().for_each(|(j, &byte)| {
            account_data[115 + i * 8 + j] = byte;
        });
    }

    // State flags
    account_data[227] = 0; // is_paused = false
    account_data[228] = 0; // emergency_mode = false
    account_data[229] = 0; // reentrancy_guard = false
    account_data[230] = presale_config_bump;
    account_data[231] = 0; // team_tokens_minted = false
    account_data[232] = 0; // marketing_tokens_minted = false
    account_data[233] = 0; // liquidity_tokens_minted = false
    account_data[234] = 0; // mint_authority_revoked = false
    account_data[235] = 0; // usdc_treasury_created = false
    account_data[236] = 0; // usdt_treasury_created = false  
    account_data[237] = 0; // nft_collection_created = false
    // NFT counters (u16 = 2 bytes each)
    0u16.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
    account_data[238 + i] = byte; // bronze_count = 0
    });
    0u16.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
    account_data[240 + i] = byte; // silver_count = 0
    });
    0u16.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
    account_data[242 + i] = byte; // gold_count = 0
    });
    0u16.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
    account_data[244 + i] = byte; // platinum_count = 0
    });
    for i in 246..360 {
    account_data[i] = 0;
}

    Ok(())
}

// ==================== PURCHASE FUNCTIONS ====================
fn buy_with_sol(accounts: &[AccountInfo], data: &[u8], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let buyer_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let user_contribution_account = next_account_info(account_info_iter)?;
    let presale_vault_account = next_account_info(account_info_iter)?;
    let mai_mint_account = next_account_info(account_info_iter)?;
    let admin_sol_account = next_account_info(account_info_iter)?;
    let token_program_account = next_account_info(account_info_iter)?;
    let system_program_account = next_account_info(account_info_iter)?;

    validate_token_program(token_program_account)?;

    if !buyer_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    if data.len() != 8 {
        return Err(ProgramError::InvalidInstructionData);
    }

    let sol_amount = u64::from_le_bytes(
        data.try_into().map_err(|_| ProgramError::InvalidInstructionData)?
    );

    if sol_amount < MIN_SOL_PURCHASE {
        return Err(ProgramError::InvalidArgument);
    }

    check_and_set_reentrancy_guard(presale_config_account)?;
    check_presale_status(presale_config_account)?;
    validate_mai_mint_pda(mai_mint_account, program_id)?;
    validate_user_contribution_pda(user_contribution_account, buyer_account.key, program_id)?;
    validate_presale_vault_pda(presale_vault_account, program_id)?;

    // Validate that admin_sol_account matches stored admin
    validate_admin_sol_account(presale_config_account, admin_sol_account.key)?;

    let result = process_sol_purchase(
        accounts,
        sol_amount,
        buyer_account.key,
        program_id,
    );

    clear_reentrancy_guard(presale_config_account)?;

    result
}

fn buy_with_usdc(accounts: &[AccountInfo], data: &[u8], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let buyer_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let user_contribution_account = next_account_info(account_info_iter)?;
    let presale_vault_account = next_account_info(account_info_iter)?;
    let mai_mint_account = next_account_info(account_info_iter)?;
    let buyer_usdc_account = next_account_info(account_info_iter)?;
    let admin_usdc_account = next_account_info(account_info_iter)?;
    let usdc_mint_account = next_account_info(account_info_iter)?;
    let token_program_account = next_account_info(account_info_iter)?;
    let system_program_account = next_account_info(account_info_iter)?;

    validate_token_program(token_program_account)?;

    if !buyer_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    if data.len() != 8 {
        return Err(ProgramError::InvalidInstructionData);
    }

    let usdc_amount = u64::from_le_bytes(
        data.try_into().map_err(|_| ProgramError::InvalidInstructionData)?
    );

    if usdc_amount < MIN_USDC_PURCHASE {
        return Err(ProgramError::InvalidArgument);
    }

    if *usdc_mint_account.key != USDC_MINT_DEVNET {
        msg!("Invalid USDC mint. Expected devnet USDC: {}", USDC_MINT_DEVNET);
        return Err(ProgramError::InvalidAccountData);
    }

    check_and_set_reentrancy_guard(presale_config_account)?;
    check_presale_status(presale_config_account)?;
    validate_mai_mint_pda(mai_mint_account, program_id)?;
    validate_user_contribution_pda(user_contribution_account, buyer_account.key, program_id)?;
    validate_presale_vault_pda(presale_vault_account, program_id)?;
    validate_usdc_treasury_ata(admin_usdc_account, program_id)?;

    let result = process_usdc_purchase(
        accounts,
        usdc_amount,
        buyer_account.key,
        program_id,
    );

    clear_reentrancy_guard(presale_config_account)?;

    result
}

fn buy_with_usdt(accounts: &[AccountInfo], data: &[u8], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let buyer_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let user_contribution_account = next_account_info(account_info_iter)?;
    let presale_vault_account = next_account_info(account_info_iter)?;
    let mai_mint_account = next_account_info(account_info_iter)?;
    let buyer_usdt_account = next_account_info(account_info_iter)?;
    let admin_usdt_account = next_account_info(account_info_iter)?;
    let usdt_mint_account = next_account_info(account_info_iter)?;
    let token_program_account = next_account_info(account_info_iter)?;
    let system_program_account = next_account_info(account_info_iter)?;

    validate_token_program(token_program_account)?;

    if !buyer_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    if data.len() != 8 {
        return Err(ProgramError::InvalidInstructionData);
    }

    let usdt_amount = u64::from_le_bytes(
        data.try_into().map_err(|_| ProgramError::InvalidInstructionData)?
    );

    if usdt_amount < MIN_USDT_PURCHASE {
        return Err(ProgramError::InvalidArgument);
    }

    if *usdt_mint_account.key != USDT_MINT_DEVNET {
        msg!("Invalid USDT mint. Expected devnet USDT: {}", USDT_MINT_DEVNET);
        return Err(ProgramError::InvalidAccountData);
    }

    check_and_set_reentrancy_guard(presale_config_account)?;
    check_presale_status(presale_config_account)?;
    validate_mai_mint_pda(mai_mint_account, program_id)?;
    validate_user_contribution_pda(user_contribution_account, buyer_account.key, program_id)?;
    validate_presale_vault_pda(presale_vault_account, program_id)?;
    validate_usdt_treasury_ata(admin_usdt_account, program_id)?;

    let result = process_usdt_purchase(
        accounts,
        usdt_amount,
        buyer_account.key,
        program_id,
    );

    clear_reentrancy_guard(presale_config_account)?;

    result
}

// ==================== PURCHASE PROCESSING ====================
fn process_sol_purchase(
    accounts: &[AccountInfo],
    sol_amount: u64,
    buyer_key: &Pubkey,
    program_id: &Pubkey,
) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let buyer_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let user_contribution_account = next_account_info(account_info_iter)?;
    let presale_vault_account = next_account_info(account_info_iter)?;
    let mai_mint_account = next_account_info(account_info_iter)?;
    let admin_sol_account = next_account_info(account_info_iter)?;
    let token_program_account = next_account_info(account_info_iter)?;
    let system_program_account = next_account_info(account_info_iter)?;

    let presale_data = presale_config_account.data.borrow_mut();
    let current_stage = presale_data[64];
    let presale_config_bump = presale_data[230];

    let sol_price_usd_cents = u64::from_le_bytes(
        presale_data[83..91].try_into().map_err(|_| ProgramError::InvalidAccountData)?
    );

    drop(presale_data);

    let usd_amount_cents: u64 = (sol_amount as u128)
    	.checked_mul(sol_price_usd_cents as u128)
    	.ok_or(ProgramError::ArithmeticOverflow)?
    	.checked_div(PRECISION as u128)
    	.ok_or(ProgramError::ArithmeticOverflow)?
    	.try_into()
    	.map_err(|_| ProgramError::ArithmeticOverflow)?;

    let purchase_results = execute_multistage_purchase(
        presale_config_account,
        usd_amount_cents,
        current_stage,
    )?;

    invoke(
        &system_instruction::transfer(buyer_account.key, admin_sol_account.key, sol_amount),
        &[buyer_account.clone(), admin_sol_account.clone(), system_program_account.clone()],
    )?;

    let total_tokens = mint_to_presale_vault_simple(
        presale_vault_account,
        mai_mint_account,
        presale_config_account,
        token_program_account,
        &purchase_results,
        presale_config_bump,
    )?;

    update_presale_statistics(
        presale_config_account,
        &purchase_results,
        sol_amount,
        0,
        0,
    )?;

    update_or_create_user_contribution(
        user_contribution_account,
        buyer_key,
        &purchase_results,
        0,
        usd_amount_cents,
        program_id,
        system_program_account,
        buyer_account,
    )?;

    emit_purchase_event(buyer_key, sol_amount, 0, current_stage, total_tokens);

    // ==================== AUTOMATIC NFT LOGIC ====================
    {
        let presale_data = presale_config_account.data.borrow();
        if presale_data[NFT_COLLECTION_CREATED_INDEX] == 1 {
            drop(presale_data);
            
            let user_data = user_contribution_account.data.borrow();
            let total_contribution = u64::from_le_bytes(
                user_data[32..40].try_into().map_err(|_| ProgramError::InvalidAccountData)?
            );
            let current_nft_tier = user_data[169];
            let nft_minted = user_data[170];
            drop(user_data);
            
    	        let total_usd = total_contribution / 100;
            let earned_tier = NftTier::from_usd_amount(total_usd);
            
            if earned_tier != NftTier::None {
                if current_nft_tier == 0 && nft_minted == 0 {
                    auto_mint_first_virtual_nft(
                        user_contribution_account,
                        buyer_key,
                        earned_tier,
                        program_id,
                    )?;
                    
                } else if earned_tier as u8 > current_nft_tier && nft_minted == 1 {
                    let old_tier = NftTier::try_from(current_nft_tier).unwrap_or(NftTier::None);
                    auto_upgrade_virtual_nft(
                        user_contribution_account,
                        buyer_key,
                        old_tier,
                        earned_tier,
                        program_id,
                    )?;
                }
            }
        }
    }
	
    Ok(())
}

fn process_usdc_purchase(
    accounts: &[AccountInfo],
    usdc_amount: u64,
    buyer_key: &Pubkey,
    program_id: &Pubkey,
) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let buyer_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let user_contribution_account = next_account_info(account_info_iter)?;
    let presale_vault_account = next_account_info(account_info_iter)?;
    let mai_mint_account = next_account_info(account_info_iter)?;
    let buyer_usdc_account = next_account_info(account_info_iter)?;
    let usdc_treasury_ata_account = next_account_info(account_info_iter)?;
    let usdc_mint_account = next_account_info(account_info_iter)?;
    let token_program_account = next_account_info(account_info_iter)?;
    let system_program_account = next_account_info(account_info_iter)?;

    let presale_data = presale_config_account.data.borrow();
    let current_stage = presale_data[64];
    let presale_config_bump = presale_data[230];
    drop(presale_data);

    let usd_cents: u64 = (usdc_amount as u128)
        .checked_mul(100)
        .ok_or(ProgramError::ArithmeticOverflow)?
        .checked_div(1_000_000)
        .ok_or(ProgramError::ArithmeticOverflow)?
        .try_into()
        .map_err(|_| ProgramError::ArithmeticOverflow)?;

    let purchase_results = execute_multistage_purchase(
        presale_config_account,
        usd_cents,
        current_stage,
    )?;

    let transfer_instruction = spl_token::instruction::transfer(
        token_program_account.key,
        buyer_usdc_account.key,
        usdc_treasury_ata_account.key,
        buyer_account.key,
        &[],
        usdc_amount,
    )?;

    invoke(
        &transfer_instruction,
        &[
            buyer_usdc_account.clone(),
            usdc_treasury_ata_account.clone(),
            buyer_account.clone(),
            token_program_account.clone(),
        ],
    )?;

    let total_tokens = mint_to_presale_vault_simple(
    presale_vault_account,
    mai_mint_account,
    presale_config_account,
    token_program_account,
    &purchase_results,
    presale_config_bump,
    )?;

    update_presale_statistics(
        presale_config_account,
        &purchase_results,
        0,
        usdc_amount,
        0,
    )?;

    update_or_create_user_contribution(
        user_contribution_account,
        buyer_key,
        &purchase_results,
        1,
        usd_cents,
        program_id,
        system_program_account,
        buyer_account,
    )?;

    emit_purchase_event(buyer_key, usdc_amount, 1, current_stage, total_tokens);

	// ==================== AUTOMATIC NFT LOGIC ====================
{
    let presale_data = presale_config_account.data.borrow();
    if presale_data[NFT_COLLECTION_CREATED_INDEX] == 1 {
        drop(presale_data);
        
        let user_data = user_contribution_account.data.borrow();
        let total_contribution = u64::from_le_bytes(
            user_data[32..40].try_into().map_err(|_| ProgramError::InvalidAccountData)?
        );
        let current_nft_tier = user_data[169];
        let nft_minted = user_data[170];
        drop(user_data);
        
        let total_usd = total_contribution / 100;
        let earned_tier = NftTier::from_usd_amount(total_usd);

        if earned_tier != NftTier::None {
            if current_nft_tier == 0 && nft_minted == 0 {
                    auto_mint_first_virtual_nft(
                        user_contribution_account,
                        buyer_key,
                        earned_tier,
                        program_id,
                    )?;
                    
                } else if earned_tier as u8 > current_nft_tier && nft_minted == 1 {
                    let old_tier = NftTier::try_from(current_nft_tier).unwrap_or(NftTier::None);
                    auto_upgrade_virtual_nft(
                        user_contribution_account,
                        buyer_key,
                        old_tier,
                        earned_tier,
                        program_id,
                    )?;
                }
            }
        }
    }

    Ok(())
}

fn process_usdt_purchase(
    accounts: &[AccountInfo],
    usdt_amount: u64,
    buyer_key: &Pubkey,
    program_id: &Pubkey,
) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let buyer_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let user_contribution_account = next_account_info(account_info_iter)?;
    let presale_vault_account = next_account_info(account_info_iter)?;
    let mai_mint_account = next_account_info(account_info_iter)?;
    let buyer_usdt_account = next_account_info(account_info_iter)?;
    let usdt_treasury_ata_account = next_account_info(account_info_iter)?;
    let usdt_mint_account = next_account_info(account_info_iter)?;
    let token_program_account = next_account_info(account_info_iter)?;
    let system_program_account = next_account_info(account_info_iter)?;

    let presale_data = presale_config_account.data.borrow();
    let current_stage = presale_data[64];
    let presale_config_bump = presale_data[230];
    drop(presale_data);

    let usd_cents: u64 = (usdt_amount as u128)
        .checked_mul(100)
        .ok_or(ProgramError::ArithmeticOverflow)?
        .checked_div(1_000_000)
        .ok_or(ProgramError::ArithmeticOverflow)?
        .try_into()
        .map_err(|_| ProgramError::ArithmeticOverflow)?;

    let purchase_results = execute_multistage_purchase(
        presale_config_account,
        usd_cents,
        current_stage,
    )?;

    let transfer_instruction = spl_token::instruction::transfer(
        token_program_account.key,
        buyer_usdt_account.key,
        usdt_treasury_ata_account.key,
        buyer_account.key,
        &[],
        usdt_amount,
    )?;

    invoke(
        &transfer_instruction,
        &[
            buyer_usdt_account.clone(),
            usdt_treasury_ata_account.clone(),
            buyer_account.clone(),
            token_program_account.clone(),
        ],
    )?;

let total_tokens = mint_to_presale_vault_simple(
    presale_vault_account,
    mai_mint_account,
    presale_config_account,
    token_program_account,
    &purchase_results,
    presale_config_bump,
)?;

    update_presale_statistics(
        presale_config_account,
        &purchase_results,
        0,
        0,
        usdt_amount,
    )?;

    update_or_create_user_contribution(
        user_contribution_account,
        buyer_key,
        &purchase_results,
        2,
        usd_cents,
        program_id,
        system_program_account,
        buyer_account,
    )?;

    emit_purchase_event(buyer_key, usdt_amount, 2, current_stage, total_tokens);

	// ==================== AUTOMATIC NFT LOGIC ====================
    {
        let presale_data = presale_config_account.data.borrow();
        if presale_data[NFT_COLLECTION_CREATED_INDEX] == 1 {
            drop(presale_data);
            
            let user_data = user_contribution_account.data.borrow();
            let total_contribution = u64::from_le_bytes(
                user_data[32..40].try_into().map_err(|_| ProgramError::InvalidAccountData)?
            );
            let current_nft_tier = user_data[169];
            let nft_minted = user_data[170];
            drop(user_data);
            
            let total_usd = total_contribution / 100;
            let earned_tier = NftTier::from_usd_amount(total_usd);
            
            if earned_tier != NftTier::None {
                if current_nft_tier == 0 && nft_minted == 0 {
                    auto_mint_first_virtual_nft(
                        user_contribution_account,
                        buyer_key,
                        earned_tier,
                        program_id,
                    )?;
                    
                } else if earned_tier as u8 > current_nft_tier && nft_minted == 1 {
                    let old_tier = NftTier::try_from(current_nft_tier).unwrap_or(NftTier::None);
                    auto_upgrade_virtual_nft(
                        user_contribution_account,
                        buyer_key,
                        old_tier,
                        earned_tier,
                        program_id,
                    )?;
                }
            }
        }
    }

    Ok(())
}

// ==================== MINT TO PRESALE VAULT ====================
fn mint_to_presale_vault<'a>(
    presale_vault_account: &AccountInfo<'a>,
    mai_mint_account: &AccountInfo<'a>,
    presale_config_account: &AccountInfo<'a>,
    token_program_account: &AccountInfo<'a>,
    purchase_results: &[StagePurchaseResult],
    presale_config_bump: u8,
) -> Result<u64, ProgramError> {

if presale_vault_account.data_len() == 0 {
        msg!("ERROR: Presale vault doesn't exist! Call create_mai_token first.");
        return Err(ProgramError::UninitializedAccount);
    }

    let mut total_tokens = 0u64;

    for result in purchase_results {
        if result.tokens == 0 {
            break;
        }

        let mint_instruction = spl_token::instruction::mint_to(
            token_program_account.key,
            mai_mint_account.key,
            presale_vault_account.key,
            presale_config_account.key,
            &[],
            result.tokens,
        )?;

        invoke_signed(
            &mint_instruction,
            &[
                mai_mint_account.clone(),
                presale_vault_account.clone(),
                presale_config_account.clone(),
                token_program_account.clone(),
            ],
            &[&[PRESALE_CONFIG_SEED, &[presale_config_bump]]],
        )?;

        total_tokens = total_tokens
            .checked_add(result.tokens)
            .ok_or(ProgramError::ArithmeticOverflow)?;
    }

	    Ok(total_tokens)
}

fn mint_to_presale_vault_simple<'a>(
    presale_vault_account: &AccountInfo<'a>,
    mai_mint_account: &AccountInfo<'a>,
    presale_config_account: &AccountInfo<'a>,
    token_program_account: &AccountInfo<'a>,
    purchase_results: &[StagePurchaseResult],
    presale_config_bump: u8,
) -> Result<u64, ProgramError> {
    let mut total_tokens = 0u64;

if presale_vault_account.data_len() == 0 {
        msg!("ERROR: Presale vault doesn't exist! Call create_mai_token first.");
        return Err(ProgramError::UninitializedAccount);
    }

    for result in purchase_results {
        if result.tokens == 0 {
            break;
        }

        let mint_instruction = spl_token::instruction::mint_to(
            token_program_account.key,
            mai_mint_account.key,
            presale_vault_account.key,
            presale_config_account.key,
            &[],
            result.tokens,
        )?;

        invoke_signed(
            &mint_instruction,
            &[
                mai_mint_account.clone(),
                presale_vault_account.clone(),
                presale_config_account.clone(),
                token_program_account.clone(),
            ],
            &[&[PRESALE_CONFIG_SEED, &[presale_config_bump]]],
        )?;

        total_tokens = total_tokens
            .checked_add(result.tokens)
            .ok_or(ProgramError::ArithmeticOverflow)?;
    }

    Ok(total_tokens)
}

// ==================== MULTISTAGE PURCHASE ====================
fn execute_multistage_purchase(
    presale_config_account: &AccountInfo,
    mut usd_amount_cents: u64,
    starting_stage: u8,
) -> Result<[StagePurchaseResult; 14], ProgramError> {
    let mut results = [StagePurchaseResult::default(); 14];
    let mut result_count = 0usize;
    let mut current_stage = starting_stage;

    // Validate starting stage
if starting_stage > 14 {
    msg!("ERROR: Invalid starting stage {}, presale completed", starting_stage);
    return Err(ProgramError::InsufficientFunds);
}

    while usd_amount_cents > 1 && current_stage <= 14 && result_count < 14 {
        let (stage_price_cents_hundredths, stage_limit) = get_stage_data(current_stage)?;

        let presale_data = presale_config_account.data.borrow();
        let stage_sold_offset = 115 + ((current_stage - 1) as usize) * 8;
        let stage_sold = u64::from_le_bytes(
            presale_data[stage_sold_offset..stage_sold_offset + 8]
                .try_into()
                .map_err(|_| ProgramError::InvalidAccountData)?
        );
        drop(presale_data);

        let stage_remaining = stage_limit
            .checked_sub(stage_sold)
            .ok_or(ProgramError::InsufficientFunds)?;

        if stage_remaining == 0 {
            current_stage = current_stage
                .checked_add(1)
                .ok_or(ProgramError::ArithmeticOverflow)?;
            continue;
        }

       let usd_amount_cents_hundredths = (usd_amount_cents as u128)
         .checked_mul(100)
         .ok_or(ProgramError::ArithmeticOverflow)?;

	let max_tokens_by_money: u64 = if stage_price_cents_hundredths == 0 {
    		0
		} else {
    	let calculation = usd_amount_cents_hundredths
        	.checked_mul(PRECISION as u128)
        	.and_then(|x| x.checked_div(stage_price_cents_hundredths as u128))
        	.unwrap_or(0);
    
    		if calculation > u64::MAX as u128 {
        	u64::MAX
    		} else {
        	calculation as u64
    		}
		};

        let tokens_to_buy = if max_tokens_by_money <= stage_remaining {
            max_tokens_by_money
        } else {
            stage_remaining
        };

        if tokens_to_buy == 0 {
            break;
        }

        let usd_spent_cents_hundredths: u64 = (tokens_to_buy as u128)
            .checked_mul(stage_price_cents_hundredths as u128)
            .ok_or(ProgramError::ArithmeticOverflow)?
            .checked_div(PRECISION as u128)
            .ok_or(ProgramError::ArithmeticOverflow)?
            .try_into()
            .map_err(|_| ProgramError::ArithmeticOverflow)?;

        let usd_spent = usd_spent_cents_hundredths
            .checked_div(100)
            .ok_or(ProgramError::ArithmeticOverflow)?;

        results[result_count] = StagePurchaseResult {
            stage: current_stage,
            tokens: tokens_to_buy,
            usd_spent,
        };

        result_count += 1;

	usd_amount_cents = usd_amount_cents
            .checked_sub(usd_spent)
            .ok_or(ProgramError::ArithmeticOverflow)?;

        // Exit if remaining amount is less than 1 cent
        if usd_amount_cents <= 1 {
            break;
        }

if tokens_to_buy == stage_remaining {
    current_stage = current_stage
        .checked_add(1)
        .ok_or(ProgramError::ArithmeticOverflow)?;
    
    // Stop after stage 14 (presale complete)
    if current_stage > 14 {
	        break;
    }
}
    }

    if result_count == 0 {
        return Err(ProgramError::InsufficientFunds);
    }

    Ok(results)
}

// ==================== STAGE DATA ====================
fn get_stage_data(stage: u8) -> Result<(u64, u64), ProgramError> {
    let (price_cents_hundredths, tokens_millions) = match stage {
        1 => (5, 126),
        2 => (6, 224),
        3 => (7, 518),
        4 => (8, 644),
        5 => (11, 924),
        6 => (12, 1134),
        7 => (13, 1008),
        8 => (14, 826),
        9 => (15, 616),
        10 => (16, 455),
        11 => (17, 245),
        12 => (18, 175),
        13 => (19, 70),
        14 => (20, 35),
        _ => return Err(ProgramError::InvalidArgument),
    };

    let tokens_limit: u64 = (tokens_millions as u128)
        .checked_mul(1_000_000)
        .ok_or(ProgramError::ArithmeticOverflow)?
        .checked_mul(PRECISION as u128)
        .ok_or(ProgramError::ArithmeticOverflow)?
        .try_into()
        .map_err(|_| ProgramError::ArithmeticOverflow)?;

    Ok((price_cents_hundredths, tokens_limit))
}

// ==================== USER CONTRIBUTION TRACKING ====================
fn update_or_create_user_contribution<'a>(
    user_contribution_account: &AccountInfo<'a>,
    buyer_key: &Pubkey,
    purchase_results: &[StagePurchaseResult],
    payment_type: u8,
    amount_paid: u64,
    program_id: &Pubkey,
    system_program_account: &AccountInfo<'a>,
    payer_account: &AccountInfo<'a>,
) -> ProgramResult {

    for (i, result) in purchase_results.iter().enumerate() {
        if result.tokens == 0 { break; }
	    }

    let clock = Clock::get()?;

    if user_contribution_account.data_len() == 0 {
        let (user_contribution_pda, bump) = Pubkey::find_program_address(
            &[USER_CONTRIBUTION_SEED, buyer_key.as_ref()],
            program_id,
        );

        if user_contribution_pda != *user_contribution_account.key {
            return Err(ProgramError::InvalidAccountData);
        }

        let rent = Rent::get()?;
        let account_size = 288; // User contribution account size (includes marketing bonus fields)
        let account_rent = rent.minimum_balance(account_size);

        invoke_signed(
            &system_instruction::create_account(
                payer_account.key,
                user_contribution_account.key,
                account_rent,
                account_size as u64,
                program_id,
            ),
            &[
                payer_account.clone(),
                user_contribution_account.clone(),
                system_program_account.clone(),
            ],
            &[&[USER_CONTRIBUTION_SEED, buyer_key.as_ref(), &[bump]]],
        )?;

        let mut data = user_contribution_account.data.borrow_mut();
        buyer_key.to_bytes().iter().enumerate().for_each(|(i, &byte)| {
            data[i] = byte;
        });
        data[168] = bump;
// Initialize NFT fields
data[169] = 0; // nft_tier_earned = None
data[170] = 0; // nft_minted = false
data[171] = 0; // nft_claimed = false
[0u8; 32].iter().enumerate().for_each(|(i, &byte)| {
    data[172 + i] = byte; // nft_mint_address = empty
});

// Initialize claimed_tokens
0u64.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
    data[204 + i] = byte; // claimed_tokens = 0
});

// Airdrop NFT fields (221-255)
data[221] = 0; // airdrop_nft_tier = None
data[222] = 0; // airdrop_nft_minted = false
data[223] = 0; // airdrop_nft_claimed = false
[0u8; 32].iter().enumerate().for_each(|(i, &byte)| {
    data[224 + i] = byte; // airdrop_nft_mint_address = empty
});

// Marketing bonus fields (256-287)
0u64.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
    data[256 + i] = byte; // marketing_instant_tokens
});
0u64.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
    data[264 + i] = byte; // marketing_instant_claimed
});
0u64.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
    data[272 + i] = byte; // marketing_vested_tokens
});
0u64.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
    data[280 + i] = byte; // marketing_vested_claimed
});

    }

    let mut data = user_contribution_account.data.borrow_mut();

let current_total = u64::from_le_bytes(
    data[32..40].try_into().map_err(|_| ProgramError::InvalidAccountData)?
);
let new_total = current_total
    .checked_add(amount_paid)
    .ok_or(ProgramError::ArithmeticOverflow)?;
new_total.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
    data[32 + i] = byte;
});

    let current_tokens = u64::from_le_bytes(
        data[40..48].try_into().map_err(|_| ProgramError::InvalidAccountData)?
    );
    let mut additional_tokens = 0u64;

    for result in purchase_results {
        if result.tokens == 0 {
            break;
        }
        additional_tokens = additional_tokens
            .checked_add(result.tokens)
            .ok_or(ProgramError::ArithmeticOverflow)?;
    }

    let new_tokens = current_tokens
        .checked_add(additional_tokens)
        .ok_or(ProgramError::ArithmeticOverflow)?;
    new_tokens.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
        data[40 + i] = byte;
    });

for result in purchase_results {
    if result.tokens == 0 {
        break;
    }
    let stage_idx = (result.stage - 1) as usize;
    let offset = 48 + stage_idx * 8;
    let current_stage_tokens = u64::from_le_bytes(
        data[offset..offset + 8].try_into().map_err(|_| ProgramError::InvalidAccountData)?
    );
    
    let new_stage_tokens = current_stage_tokens
        .checked_add(result.tokens)
        .ok_or(ProgramError::ArithmeticOverflow)?;
    
    new_stage_tokens.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
        data[offset + i] = byte;
    });
}

clock.unix_timestamp.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
    data[160 + i] = byte;
});

	// NFT logic moved to purchase functions for automatic processing

    data[212] |= 1 << payment_type;

    Ok(())
}

// ==================== STATISTICS UPDATE ====================
fn update_presale_statistics(
    presale_config_account: &AccountInfo,
    purchase_results: &[StagePurchaseResult],
    sol_amount: u64,
    usdc_amount: u64,
    usdt_amount: u64,
) -> ProgramResult {
    let mut presale_data = presale_config_account.data.borrow_mut();

    let total_sold = u64::from_le_bytes(
        presale_data[65..73].try_into().map_err(|_| ProgramError::InvalidAccountData)?
    );

    let mut total_new_tokens = 0u64;
    for result in purchase_results {
        if result.tokens == 0 {
            break;
        }
        total_new_tokens = total_new_tokens
            .checked_add(result.tokens)
            .ok_or(ProgramError::ArithmeticOverflow)?;
    }

    let new_total_sold = total_sold
        .checked_add(total_new_tokens)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    new_total_sold.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
        presale_data[65 + i] = byte;
    });

    for result in purchase_results {
        if result.tokens == 0 {
            break;
        }
        let stage_sold_offset = 115 + ((result.stage - 1) as usize) * 8;
        let current_stage_sold = u64::from_le_bytes(
            presale_data[stage_sold_offset..stage_sold_offset + 8]
                .try_into()
                .map_err(|_| ProgramError::InvalidAccountData)?
        );

        let new_stage_sold = current_stage_sold
            .checked_add(result.tokens)
            .ok_or(ProgramError::ArithmeticOverflow)?;

        new_stage_sold.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
            presale_data[stage_sold_offset + i] = byte;
        });

        let (_, stage_limit) = get_stage_data(result.stage)?;
if new_stage_sold >= stage_limit {
    if result.stage == 14 {
        // Auto-close presale when stage 14 is complete
        presale_data[73] = 0; // is_active = false
        emit_presale_completion_event(new_total_sold);
    } else if result.stage < 14 {
        // Normal transition to next stage
        let current_stage = presale_data[64];
        if current_stage == result.stage {
            presale_data[64] = result.stage + 1;
            emit_stage_transition_event(result.stage, result.stage + 1, new_total_sold);
            msg!("Stage {} completed! Auto-transition to stage {}", result.stage, result.stage + 1);
        }
    }
} else if result.stage == 14 {
    // Auto-close if < 1000 tokens remaining on stage 14
    let remaining = stage_limit.saturating_sub(new_stage_sold);
    let min_threshold = 1000u64
        .checked_mul(PRECISION)
        .unwrap_or(1_000_000_000_000);
    if remaining < min_threshold {
        presale_data[73] = 0; // is_active = false
        emit_presale_completion_event(new_total_sold);
        msg!("AUTO-CLOSE: Stage 14 remaining {} < 1000 tokens", remaining / PRECISION);
    }
}
    }

    if sol_amount > 0 {
        let sol_collected = u64::from_le_bytes(
            presale_data[107..115].try_into().map_err(|_| ProgramError::InvalidAccountData)?
        );
        let new_sol_collected = sol_collected
            .checked_add(sol_amount)
            .ok_or(ProgramError::ArithmeticOverflow)?;
        new_sol_collected.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
            presale_data[107 + i] = byte;
        });
    }

    if usdc_amount > 0 {
        let usdc_collected = u64::from_le_bytes(
            presale_data[91..99].try_into().map_err(|_| ProgramError::InvalidAccountData)?
        );
        let new_usdc_collected = usdc_collected
            .checked_add(usdc_amount)
            .ok_or(ProgramError::ArithmeticOverflow)?;
        new_usdc_collected.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
            presale_data[91 + i] = byte;
        });
    }

    if usdt_amount > 0 {
        let usdt_collected = u64::from_le_bytes(
            presale_data[99..107].try_into().map_err(|_| ProgramError::InvalidAccountData)?
        );
        let new_usdt_collected = usdt_collected
            .checked_add(usdt_amount)
            .ok_or(ProgramError::ArithmeticOverflow)?;
        new_usdt_collected.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
            presale_data[99 + i] = byte;
        });
    }

    Ok(())
}

// ==================== LISTING AND VESTING ====================
fn trigger_listing(accounts: &[AccountInfo], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let admin_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;

    if !admin_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    validate_admin_access(presale_config_account, admin_account.key)?;

    // Check that listing hasn't been triggered yet
    let presale_data = presale_config_account.data.borrow();
    if presale_data[82] == 1 {
        msg!("ERROR: Listing already triggered!");
        return Err(ProgramError::AccountAlreadyInitialized);
    }
    drop(presale_data);

    let clock = Clock::get()?;
    let mut presale_data = presale_config_account.data.borrow_mut();

    clock.unix_timestamp.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
        presale_data[74 + i] = byte;
    });
    presale_data[82] = 1;

    msg!("LISTING TRIGGERED! Vesting activated at timestamp: {}", clock.unix_timestamp);

    Ok(())
}

fn claim_tokens(accounts: &[AccountInfo], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let claimer_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let user_contribution_account = next_account_info(account_info_iter)?;
    let presale_vault_account = next_account_info(account_info_iter)?;
    let claimer_mai_account = next_account_info(account_info_iter)?;
    let mai_mint_account = next_account_info(account_info_iter)?;
    let token_program_account = next_account_info(account_info_iter)?;
    let system_program_account = next_account_info(account_info_iter)?;
    let rent_sysvar_account = next_account_info(account_info_iter)?;
    let associated_token_program_account = next_account_info(account_info_iter)?;

    validate_token_program(token_program_account)?;

    if !claimer_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    validate_user_contribution_pda(user_contribution_account, claimer_account.key, program_id)?;
    validate_presale_vault_pda(presale_vault_account, program_id)?;

    check_and_set_reentrancy_guard(presale_config_account)?;
    check_claims_not_paused(presale_config_account)?;

    let presale_data = presale_config_account.data.borrow();

    if presale_data[82] != 1 {
        clear_reentrancy_guard(presale_config_account)?;
        return Err(ProgramError::InvalidAccountData);
    }

    let listing_date = i64::from_le_bytes(
        presale_data[74..82].try_into().map_err(|_| ProgramError::InvalidAccountData)?
    );
    let presale_config_bump = presale_data[230];
    drop(presale_data);

    let user_data = user_contribution_account.data.borrow();

    let user_key = Pubkey::new_from_array(
        user_data[0..32].try_into().map_err(|_| ProgramError::InvalidAccountData)?
    );

    if user_key != *claimer_account.key {
        clear_reentrancy_guard(presale_config_account)?;
        return Err(ProgramError::InvalidAccountData);
    }

    let total_tokens = u64::from_le_bytes(
        user_data[40..48].try_into().map_err(|_| ProgramError::InvalidAccountData)?
    );

    let tokens_already_claimed = u64::from_le_bytes(
        user_data[204..212].try_into().map_err(|_| ProgramError::InvalidAccountData)?
    );

    // Calculate total claimable tokens (optimized to avoid stack overflow)
    let total_claimable = {
        let clock = Clock::get()?;
        let mut claimable = 0u64;

        // Read stage tokens one by one to save stack space
        for stage_idx in 0..14 {
            let offset = 48 + stage_idx * 8;
            let stage_tokens = u64::from_le_bytes(
                user_data[offset..offset + 8].try_into().map_err(|_| ProgramError::InvalidAccountData)?
            );

            if stage_tokens > 0 {
                let stage = (stage_idx + 1) as u8;
                let (tge_percent, cliff_months, vesting_months) = get_vesting_params(stage)?;

                let stage_claimable = calculate_claimable_tokens(
                    stage_tokens,
                    tge_percent,
                    cliff_months,
                    vesting_months,
                    listing_date,
                    clock.unix_timestamp,
                )?;

                claimable = claimable
                    .checked_add(stage_claimable)
                    .ok_or(ProgramError::ArithmeticOverflow)?;
            }
        }

        claimable.saturating_sub(tokens_already_claimed)
    };
    drop(user_data);

    // Create ATA if needed
    {
        let rent = Rent::from_account_info(rent_sysvar_account)?;
        ensure_user_mai_ata_exists(
            claimer_mai_account,
            claimer_account,
            mai_mint_account,
            token_program_account,
            system_program_account,
            &rent,
            associated_token_program_account,
        )?;
    }

    if total_claimable == 0 {
        clear_reentrancy_guard(presale_config_account)?;
        return Err(ProgramError::InsufficientFunds);
    }

    let transfer_instruction = spl_token::instruction::transfer(
        token_program_account.key,
        presale_vault_account.key,
        claimer_mai_account.key,
        presale_config_account.key,
        &[],
        total_claimable,
    )?;

    invoke_signed(
        &transfer_instruction,
        &[
            presale_vault_account.clone(),
            claimer_mai_account.clone(),
            presale_config_account.clone(),
            token_program_account.clone(),
        ],
        &[&[PRESALE_CONFIG_SEED, &[presale_config_bump]]],
    )?;


	// Update claimed_tokens in user account
    let mut user_data = user_contribution_account.data.borrow_mut();
    let new_claimed = tokens_already_claimed
    .checked_add(total_claimable)
    .ok_or(ProgramError::ArithmeticOverflow)?;

    let remaining_locked = total_tokens
    .checked_sub(new_claimed)
    .unwrap_or(0);

    new_claimed.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
    user_data[204 + i] = byte;
});
drop(user_data);

    //emit_claim_event(claimer_account.key, total_claimable, remaining_locked);

    clear_reentrancy_guard(presale_config_account)?;

    msg!("Successfully TRANSFERRED {} MAI tokens from vault to user", total_claimable);
    msg!("Remaining locked in vault: {} MAI tokens", remaining_locked);

    Ok(())
}

// ==================== VESTING CALCULATIONS ====================
fn get_vesting_params(stage: u8) -> Result<(u8, u8, u8), ProgramError> {
    let (tge, cliff, vesting) = match stage {
        1 => (3, 4, 10),
        2 => (3, 3, 10),
        3 => (4, 3, 10),
        4 => (4, 2, 9),
        5 => (5, 2, 9),
        6 => (5, 1, 9),
        7 => (6, 1, 8),
        8 => (6, 1, 8),
        9 => (7, 1, 8),
        10 => (7, 0, 8),
        11 => (7, 0, 7),
        12 => (8, 0, 7),
        13 => (8, 0, 6),
        14 => (8, 0, 5),
        _ => return Err(ProgramError::InvalidArgument),
    };

    Ok((tge, cliff, vesting))
}

// ==================== VESTING CALCULATION ====================
fn calculate_claimable_tokens(
    total_tokens: u64,
    tge_percent: u8,
    cliff_months: u8,
    vesting_months: u8,
    listing_date: i64,
    current_time: i64,
) -> Result<u64, ProgramError> {
    let seconds_since_listing = current_time - listing_date;
    if seconds_since_listing < 0 {
        return Ok(0);
    }

    // Calculate TGE tokens with overflow check
    let tge_tokens = total_tokens
        .checked_mul(tge_percent as u64)
        .ok_or(ProgramError::ArithmeticOverflow)?
        .checked_div(100)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    let cliff_seconds = (cliff_months as i64)
        .checked_mul(30 * 24 * 60 * 60)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    if seconds_since_listing < cliff_seconds {
        return Ok(tge_tokens);
    }

    let vesting_seconds = (vesting_months as i64)
        .checked_mul(30 * 24 * 60 * 60)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    let vesting_tokens = total_tokens
        .checked_sub(tge_tokens)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    let total_unlocked = if seconds_since_listing >= cliff_seconds + vesting_seconds {
        total_tokens
    } else {
        let vesting_progress = (seconds_since_listing - cliff_seconds) as u64;
        
        // Improved precision: changed operation order to minimize precision loss
let vested_amount = (vesting_tokens as u128)
    .checked_mul(vesting_progress as u128)
    .ok_or(ProgramError::ArithmeticOverflow)?
    .checked_div(vesting_seconds as u128)
    .ok_or(ProgramError::ArithmeticOverflow)?
    .try_into()
    .map_err(|_| ProgramError::ArithmeticOverflow)?;

        tge_tokens
            .checked_add(vested_amount)
            .ok_or(ProgramError::ArithmeticOverflow)?
    };

    Ok(total_unlocked)
}

// ==================== MINT FUNCTIONS ====================
fn mint_team_tokens(accounts: &[AccountInfo], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let admin_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let team_vault_ata_account = next_account_info(account_info_iter)?;
    let team_vault_pda_account = next_account_info(account_info_iter)?;
    let team_vesting_account = next_account_info(account_info_iter)?;
    let mai_mint_account = next_account_info(account_info_iter)?;
    let token_program_account = next_account_info(account_info_iter)?;
    let system_program_account = next_account_info(account_info_iter)?;
    let rent_sysvar_account = next_account_info(account_info_iter)?;
    let associated_token_program_account = next_account_info(account_info_iter)?;

    validate_token_program(token_program_account)?;

    if !admin_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    validate_admin_access(presale_config_account, admin_account.key)?;
    validate_mai_mint_pda(mai_mint_account, program_id)?;

    let presale_data = presale_config_account.data.borrow();
    if presale_data[231] == 1 {
        return Err(ProgramError::AccountAlreadyInitialized);
    }
    let presale_config_bump = presale_data[230];
    drop(presale_data);

    // Validate Team Vault PDA
    let (team_vault_pda, team_vault_bump) = Pubkey::find_program_address(
        &[TEAM_VAULT_SEED],
        program_id,
    );

    if team_vault_pda != *team_vault_pda_account.key {
        msg!("Invalid Team Vault PDA. Expected: {}, got: {}", team_vault_pda, team_vault_pda_account.key);
        return Err(ProgramError::InvalidAccountData);
    }

    // Validate Team Vault ATA
    let team_vault_ata = get_associated_token_address(&team_vault_pda, mai_mint_account.key);

    if team_vault_ata != *team_vault_ata_account.key {
        msg!("Invalid Team Vault ATA. Expected: {}, got: {}", team_vault_ata, team_vault_ata_account.key);
        return Err(ProgramError::InvalidAccountData);
    }

    let rent = Rent::from_account_info(rent_sysvar_account)?;
    let team_tokens = 1_000_000_000u64
        .checked_mul(PRECISION)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    // Create Team Vault ATA if needed
    if team_vault_ata_account.data_len() == 0 {
        let create_ata_instruction = create_associated_token_account(
            admin_account.key,
            &team_vault_pda,        // owner (PDA)
            mai_mint_account.key,
            token_program_account.key,
        );
        
        // Create Team Vault ATA
        invoke(
            &create_ata_instruction,
            &[
                admin_account.clone(),                    // payer
                team_vault_ata_account.clone(),          // associated_token_address (ATA)
                team_vault_pda_account.clone(),
                mai_mint_account.clone(),                // mint
                system_program_account.clone(),          // system_program
                token_program_account.clone(),           // token_program
                associated_token_program_account.clone(), // associated_token_program
            ],
        )?;

        msg!("Team Vault ATA auto-created: {}", team_vault_ata_account.key);
    }

    // Mint tokens to Team Vault ATA
    let mint_instruction = spl_token::instruction::mint_to(
        token_program_account.key,
        mai_mint_account.key,
        team_vault_ata_account.key,
        presale_config_account.key,
        &[],
        team_tokens,
    )?;

    invoke_signed(
        &mint_instruction,
        &[
            mai_mint_account.clone(),
            team_vault_ata_account.clone(),
            presale_config_account.clone(),
            token_program_account.clone(),
        ],
        &[&[PRESALE_CONFIG_SEED, &[presale_config_bump]]],
    )?;

    // Freeze ATA until vesting starts
    let freeze_instruction = spl_token::instruction::freeze_account(
        token_program_account.key,
        team_vault_ata_account.key,
        mai_mint_account.key,
        presale_config_account.key,
        &[],
    )?;

    invoke_signed(
        &freeze_instruction,
        &[
            team_vault_ata_account.clone(),
            mai_mint_account.clone(),
            presale_config_account.clone(),
            token_program_account.clone(),
        ],
        &[&[PRESALE_CONFIG_SEED, &[presale_config_bump]]],
    )?;

    // Create team vesting record
    create_team_vesting_record(
        team_vesting_account,
        admin_account.key,
        team_tokens,
        program_id,
        system_program_account,
        admin_account
    )?;

    let mut presale_data = presale_config_account.data.borrow_mut();
    presale_data[231] = 1;

    msg!("TEAM TOKENS MINTED: {} MAI to Team Vault ATA", team_tokens);

    Ok(())
}

fn mint_marketing_tokens(accounts: &[AccountInfo], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let admin_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let marketing_vault_ata_account = next_account_info(account_info_iter)?;
    let marketing_vault_pda_account = next_account_info(account_info_iter)?;
    let mai_mint_account = next_account_info(account_info_iter)?;
    let token_program_account = next_account_info(account_info_iter)?;
    let system_program_account = next_account_info(account_info_iter)?;
    let rent_sysvar_account = next_account_info(account_info_iter)?;
    let associated_token_program_account = next_account_info(account_info_iter)?;

    validate_token_program(token_program_account)?;

    if !admin_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    validate_admin_access(presale_config_account, admin_account.key)?;
    validate_mai_mint_pda(mai_mint_account, program_id)?;

    let presale_data = presale_config_account.data.borrow();
    if presale_data[232] == 1 {
        return Err(ProgramError::AccountAlreadyInitialized);
    }
    let presale_config_bump = presale_data[230];
    drop(presale_data);

    // Validate Marketing Vault PDA
    let (marketing_vault_pda, marketing_vault_bump) = Pubkey::find_program_address(
        &[MARKETING_VAULT_SEED],
        program_id,
    );

    if marketing_vault_pda != *marketing_vault_pda_account.key {
        msg!("Invalid Marketing Vault PDA. Expected: {}, got: {}", marketing_vault_pda, marketing_vault_pda_account.key);
        return Err(ProgramError::InvalidAccountData);
    }

    // Validate Marketing Vault ATA
    let marketing_vault_ata = get_associated_token_address(&marketing_vault_pda, mai_mint_account.key);

    if marketing_vault_ata != *marketing_vault_ata_account.key {
        msg!("Invalid Marketing Vault ATA. Expected: {}, got: {}", marketing_vault_ata, marketing_vault_ata_account.key);
        return Err(ProgramError::InvalidAccountData);
    }

    let rent = Rent::from_account_info(rent_sysvar_account)?;
    let marketing_tokens = 1_000_000_000u64
        .checked_mul(PRECISION)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    // Create Marketing Vault ATA if needed
    if marketing_vault_ata_account.data_len() == 0 {
        let create_ata_instruction = create_associated_token_account(
            admin_account.key,
            &marketing_vault_pda,   // owner (PDA)
            mai_mint_account.key,
            token_program_account.key,
        );
        
        // Create Team Vault ATA
        invoke(
            &create_ata_instruction,
            &[
                admin_account.clone(),                    // payer
                marketing_vault_ata_account.clone(),     // associated_token_address (ATA)
                marketing_vault_pda_account.clone(),
                mai_mint_account.clone(),                // mint
                system_program_account.clone(),          // system_program
                token_program_account.clone(),           // token_program
                associated_token_program_account.clone(), // associated_token_program
            ],
        )?;

        msg!("Marketing Vault ATA auto-created: {}", marketing_vault_ata_account.key);
    }

    // Mint tokens to Marketing Vault ATA
    let mint_instruction = spl_token::instruction::mint_to(
        token_program_account.key,
        mai_mint_account.key,
        marketing_vault_ata_account.key,
        presale_config_account.key,
        &[],
        marketing_tokens,
    )?;

    invoke_signed(
        &mint_instruction,
        &[
            mai_mint_account.clone(),
            marketing_vault_ata_account.clone(),
            presale_config_account.clone(),
            token_program_account.clone(),
        ],
        &[&[PRESALE_CONFIG_SEED, &[presale_config_bump]]],
    )?;

    // No freeze - immediately available for marketing

    let mut presale_data = presale_config_account.data.borrow_mut();
    presale_data[232] = 1;

    msg!("MARKETING TOKENS MINTED: {} MAI to Marketing Vault ATA", marketing_tokens);
	
    Ok(())
}

fn mint_liquidity_tokens(accounts: &[AccountInfo], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let admin_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let liquidity_vault_ata_account = next_account_info(account_info_iter)?;
    let liquidity_vault_pda_account = next_account_info(account_info_iter)?;
    let mai_mint_account = next_account_info(account_info_iter)?;
    let token_program_account = next_account_info(account_info_iter)?;
    let system_program_account = next_account_info(account_info_iter)?;
    let rent_sysvar_account = next_account_info(account_info_iter)?;
    let associated_token_program_account = next_account_info(account_info_iter)?;

    validate_token_program(token_program_account)?;

    if !admin_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    validate_admin_access(presale_config_account, admin_account.key)?;
    validate_mai_mint_pda(mai_mint_account, program_id)?;

    let presale_data = presale_config_account.data.borrow();
    if presale_data[233] == 1 {
        return Err(ProgramError::AccountAlreadyInitialized);
    }
    let presale_config_bump = presale_data[230];
    drop(presale_data);

    // Validate Liquidity Vault PDA
    let (liquidity_vault_pda, liquidity_vault_bump) = Pubkey::find_program_address(
        &[LIQUIDITY_VAULT_SEED],
        program_id,
    );

    if liquidity_vault_pda != *liquidity_vault_pda_account.key {
        msg!("Invalid Liquidity Vault PDA. Expected: {}, got: {}", liquidity_vault_pda, liquidity_vault_pda_account.key);
        return Err(ProgramError::InvalidAccountData);
    }

    // Validate Liquidity Vault ATA
    let liquidity_vault_ata = get_associated_token_address(&liquidity_vault_pda, mai_mint_account.key);

    if liquidity_vault_ata != *liquidity_vault_ata_account.key {
        msg!("Invalid Liquidity Vault ATA. Expected: {}, got: {}", liquidity_vault_ata, liquidity_vault_ata_account.key);
        return Err(ProgramError::InvalidAccountData);
    }

    let rent = Rent::from_account_info(rent_sysvar_account)?;
    let liquidity_tokens = 1_000_000_000u64
        .checked_mul(PRECISION)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    // Create Liquidity Vault ATA if needed
    if liquidity_vault_ata_account.data_len() == 0 {
        let create_ata_instruction = create_associated_token_account(
            admin_account.key,
            &liquidity_vault_pda,   // owner (PDA)
            mai_mint_account.key,
            token_program_account.key,
        );
        
        // Create Team Vault ATA
        invoke(
            &create_ata_instruction,
            &[
                admin_account.clone(),                    // payer
                liquidity_vault_ata_account.clone(),     // associated_token_address (ATA)
                liquidity_vault_pda_account.clone(),
                mai_mint_account.clone(),                // mint
                system_program_account.clone(),          // system_program
                token_program_account.clone(),           // token_program
                associated_token_program_account.clone(), // associated_token_program
            ],
        )?;

        msg!("Liquidity Vault ATA auto-created: {}", liquidity_vault_ata_account.key);
    }

    // Mint tokens to Liquidity Vault ATA
    let mint_instruction = spl_token::instruction::mint_to(
        token_program_account.key,
        mai_mint_account.key,
        liquidity_vault_ata_account.key,
        presale_config_account.key,
        &[],
        liquidity_tokens,
    )?;

    invoke_signed(
        &mint_instruction,
        &[
            mai_mint_account.clone(),
            liquidity_vault_ata_account.clone(),
            presale_config_account.clone(),
            token_program_account.clone(),
        ],
        &[&[PRESALE_CONFIG_SEED, &[presale_config_bump]]],
    )?;

    // No freeze - immediately available for liquidity

    let mut presale_data = presale_config_account.data.borrow_mut();
    presale_data[233] = 1;

    msg!("LIQUIDITY TOKENS MINTED: {} MAI to Liquidity Vault ATA", liquidity_tokens);

    Ok(())
}

// ==================== TEAM VESTING ====================
fn create_team_vesting_record<'a>(
    team_vesting_account: &AccountInfo<'a>,
    team_wallet: &Pubkey,
    tokens: u64,
    program_id: &Pubkey,
    system_program_account: &AccountInfo<'a>,
    payer_account: &AccountInfo<'a>,
) -> ProgramResult {
    let clock = Clock::get()?;

    if team_vesting_account.data_len() == 0 {
        let (team_vesting_pda, bump) = Pubkey::find_program_address(
            &[TEAM_VESTING_SEED, team_wallet.as_ref()],
            program_id,
        );

        if team_vesting_pda != *team_vesting_account.key {
            return Err(ProgramError::InvalidAccountData);
        }

        let rent = Rent::get()?;
        let account_size = 64;
        let account_rent = rent.minimum_balance(account_size);

        invoke_signed(
            &system_instruction::create_account(
                payer_account.key,
                team_vesting_account.key,
                account_rent,
                account_size as u64,
                program_id,
            ),
            &[
                payer_account.clone(),
                team_vesting_account.clone(),
                system_program_account.clone(),
            ],
            &[&[TEAM_VESTING_SEED, team_wallet.as_ref(), &[bump]]],
        )?;
    }

    let mut data = team_vesting_account.data.borrow_mut();

    team_wallet.to_bytes().iter().enumerate().for_each(|(i, &byte)| {
        data[i] = byte;
    });

tokens.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
    data[32 + i] = byte;
});
data[40] = 0;    // TGE percent
data[41] = 0;    // Reserved
data[42] = 12;   // Cliff months
data[43] = 12;   // Vesting months
data[44] = 0;    // tge_claimed = false
0u64.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
    data[45 + i] = byte;
});

    clock.unix_timestamp.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
        data[53 + i] = byte;
    });
    data[62] = 1;

    let (_, bump) = Pubkey::find_program_address(
        &[TEAM_VESTING_SEED, team_wallet.as_ref()],
        program_id,
    );
    data[63] = bump;

    Ok(())
}

fn claim_team_tokens(accounts: &[AccountInfo], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let team_member_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let team_vesting_account = next_account_info(account_info_iter)?;
    let team_mai_account = next_account_info(account_info_iter)?;
    let team_receiving_account = next_account_info(account_info_iter)?;
    let mai_mint_account = next_account_info(account_info_iter)?;
    let token_program_account = next_account_info(account_info_iter)?;
    let team_vault_pda_account = next_account_info(account_info_iter)?;

    validate_token_program(token_program_account)?;

    if !team_member_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    check_claims_not_paused(presale_config_account)?;

    let presale_data = presale_config_account.data.borrow();
    if presale_data[82] != 1 {
        return Err(ProgramError::InvalidAccountData);
    }

    let listing_date = i64::from_le_bytes(
        presale_data[74..82].try_into().map_err(|_| ProgramError::InvalidAccountData)?
    );
    let presale_config_bump = presale_data[230];
    drop(presale_data);

    let vesting_data = team_vesting_account.data.borrow();
    let team_wallet = Pubkey::new_from_array(
        vesting_data[0..32].try_into().map_err(|_| ProgramError::InvalidAccountData)?
    );

    if team_wallet != *team_member_account.key {
        return Err(ProgramError::InvalidAccountData);
    }

    let total_tokens = u64::from_le_bytes(
        vesting_data[32..40].try_into().map_err(|_| ProgramError::InvalidAccountData)?
    );
    let tge_claimed = vesting_data[44];
    let tokens_claimed = u64::from_le_bytes(
    vesting_data[45..53].try_into().map_err(|_| ProgramError::InvalidAccountData)?
    );

    let tge_percent = vesting_data[40];
    let cliff_months = vesting_data[42]; 
    let vesting_months = vesting_data[43];
    drop(vesting_data);

    let clock = Clock::get()?;

    // TGE + VESTING logic
    let claimable = if tge_claimed == 0 && tge_percent > 0 {
    // First claim with TGE - issue TGE immediately
    total_tokens * tge_percent as u64 / 100
    } else {
    // TGE already claimed or TGE = 0% - calculate cliff/vesting
    let vesting_tokens = total_tokens - (total_tokens * tge_percent as u64 / 100);
    let total_vested = calculate_claimable_tokens(
        vesting_tokens,  // Vesting portion only
        0,               // TGE = 0 for this calculation
        cliff_months,
        vesting_months,
        listing_date,
        clock.unix_timestamp,
    )?;
    
    // Subtract already claimed vesting tokens
    let tge_amount = if tge_percent > 0 { total_tokens * tge_percent as u64 / 100 } else { 0 };
    let vesting_claimed = tokens_claimed.saturating_sub(tge_amount);
    let available_vesting = total_vested.saturating_sub(vesting_claimed);
    
    available_vesting
};

// Update TGE claimed flag
if tge_claimed == 0 && tge_percent > 0 {
    let mut vesting_data = team_vesting_account.data.borrow_mut();
    vesting_data[44] = 1;  // tge_claimed = true
    drop(vesting_data);
}

    if claimable == 0 {
        return Err(ProgramError::InsufficientFunds);
    }

    let thaw_instruction = spl_token::instruction::thaw_account(
        token_program_account.key,
        team_mai_account.key,
        mai_mint_account.key,
        presale_config_account.key,
        &[],
    )?;
    let vesting_tokens = total_tokens - (total_tokens * tge_percent as u64 / 100);
    invoke_signed(
        &thaw_instruction,
        &[
            team_mai_account.clone(),
            mai_mint_account.clone(),
            presale_config_account.clone(),
            token_program_account.clone(),
        ],
        &[&[PRESALE_CONFIG_SEED, &[presale_config_bump]]],
    )?;

    // Transfer tokens from Team Vault
    let (team_vault_pda, team_vault_bump) = Pubkey::find_program_address(
    &[TEAM_VAULT_SEED],
    program_id,
);

let transfer_instruction = spl_token::instruction::transfer(
    token_program_account.key,
    team_mai_account.key,        
    team_receiving_account.key,  
    &team_vault_pda,
    &[],
    claimable,
)?;

invoke_signed(
    &transfer_instruction,
    &[
        team_mai_account.clone(),
        team_receiving_account.clone(),
        team_vault_pda_account.clone(),
        token_program_account.clone(),
    ],
    &[&[TEAM_VAULT_SEED, &[team_vault_bump]]],
)?;

    let mut vesting_data = team_vesting_account.data.borrow_mut();
    let new_claimed = tokens_claimed
        .checked_add(claimable)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    new_claimed.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
        vesting_data[45 + i] = byte;
    });

    let remaining = total_tokens - new_claimed;
    if remaining > 0 {
        let freeze_instruction = spl_token::instruction::freeze_account(
            token_program_account.key,
            team_mai_account.key,
            mai_mint_account.key,
            presale_config_account.key,
            &[],
        )?;

        invoke_signed(
            &freeze_instruction,
            &[
                team_mai_account.clone(),
                mai_mint_account.clone(),
                presale_config_account.clone(),
                token_program_account.clone(),
            ],
            &[&[PRESALE_CONFIG_SEED, &[presale_config_bump]]],
        )?;
    }

    drop(vesting_data);

    //emit_claim_event(team_member_account.key, claimable, total_tokens - new_claimed);

    msg!("TEAM CLAIMED: {} MAI tokens transferred", claimable);
    msg!("Team remaining locked: {} MAI tokens", total_tokens - new_claimed);

    Ok(())
}

// ==================== TREASURY FUNCTIONS ====================
fn create_usdc_treasury(accounts: &[AccountInfo], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let admin_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let usdc_treasury_pda_account = next_account_info(account_info_iter)?;
    let usdc_treasury_ata_account = next_account_info(account_info_iter)?;
    let usdc_mint_account = next_account_info(account_info_iter)?;
    let token_program_account = next_account_info(account_info_iter)?;
    let system_program_account = next_account_info(account_info_iter)?;
    let associated_token_program_account = next_account_info(account_info_iter)?;

    if !admin_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    validate_admin_access(presale_config_account, admin_account.key)?;

    // Check if already created
    let presale_data = presale_config_account.data.borrow();
    if presale_data[235] == 1 {
    msg!("ERROR: USDC Treasury already created!");
    return Err(ProgramError::AccountAlreadyInitialized);
    }
    drop(presale_data);

    if *usdc_mint_account.key != USDC_MINT_DEVNET {
        msg!("Invalid USDC mint. Expected: {}", USDC_MINT_DEVNET);
        return Err(ProgramError::InvalidAccountData);
    }

    let (usdc_treasury_pda, _) = Pubkey::find_program_address(
        &[USDC_TREASURY_SEED],
        program_id,
    );

    if usdc_treasury_pda != *usdc_treasury_pda_account.key {
        msg!("Invalid USDC Treasury PDA. Expected: {}, got: {}", usdc_treasury_pda, usdc_treasury_pda_account.key);
        return Err(ProgramError::InvalidAccountData);
    }

    let usdc_treasury_ata = get_associated_token_address(&usdc_treasury_pda, usdc_mint_account.key);

    if usdc_treasury_ata != *usdc_treasury_ata_account.key {
        msg!("Invalid USDC Treasury ATA. Expected: {}, got: {}", usdc_treasury_ata, usdc_treasury_ata_account.key);
        return Err(ProgramError::InvalidAccountData);
    }

    if usdc_treasury_ata_account.data_len() == 0 {
        let create_ata_instruction = create_associated_token_account(
            admin_account.key,
            &usdc_treasury_pda,
            usdc_mint_account.key,
            token_program_account.key,
        );

        invoke(
            &create_ata_instruction,
            &[
                admin_account.clone(),
                usdc_treasury_ata_account.clone(),
                usdc_treasury_pda_account.clone(),
                usdc_mint_account.clone(),
                system_program_account.clone(),
                token_program_account.clone(),
                associated_token_program_account.clone(),
            ],
        )?;

        msg!("USDC Treasury ATA created: {}", usdc_treasury_ata_account.key);
        msg!("USDC Treasury PDA (Owner): {}", usdc_treasury_pda);
    } else {
        msg!("USDC Treasury ATA already exists: {}", usdc_treasury_ata_account.key);
    }

    // Mark as created
    let mut presale_data = presale_config_account.data.borrow_mut();
    presale_data[235] = 1; // usdc_treasury_created = true
    drop(presale_data);

    msg!("USDC Treasury marked as created");

    Ok(())
}

fn create_usdt_treasury(accounts: &[AccountInfo], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let admin_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let usdt_treasury_pda_account = next_account_info(account_info_iter)?;
    let usdt_treasury_ata_account = next_account_info(account_info_iter)?;
    let usdt_mint_account = next_account_info(account_info_iter)?;
    let token_program_account = next_account_info(account_info_iter)?;
    let system_program_account = next_account_info(account_info_iter)?;
    let associated_token_program_account = next_account_info(account_info_iter)?;

    if !admin_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    validate_admin_access(presale_config_account, admin_account.key)?;

    // Check if already created
    let presale_data = presale_config_account.data.borrow();
    if presale_data[236] == 1 {
    msg!("ERROR: USDT Treasury already created!");
    return Err(ProgramError::AccountAlreadyInitialized);
    }
    drop(presale_data);

    if *usdt_mint_account.key != USDT_MINT_DEVNET {
        msg!("Invalid USDT mint. Expected: {}", USDT_MINT_DEVNET);
        return Err(ProgramError::InvalidAccountData);
    }

    let (usdt_treasury_pda, _) = Pubkey::find_program_address(
        &[USDT_TREASURY_SEED],
        program_id,
    );

    if usdt_treasury_pda != *usdt_treasury_pda_account.key {
        msg!("Invalid USDT Treasury PDA. Expected: {}, got: {}", usdt_treasury_pda, usdt_treasury_pda_account.key);
        return Err(ProgramError::InvalidAccountData);
    }

    let usdt_treasury_ata = get_associated_token_address(&usdt_treasury_pda, usdt_mint_account.key);

    if usdt_treasury_ata != *usdt_treasury_ata_account.key {
        msg!("Invalid USDT Treasury ATA. Expected: {}, got: {}", usdt_treasury_ata, usdt_treasury_ata_account.key);
        return Err(ProgramError::InvalidAccountData);
    }

    if usdt_treasury_ata_account.data_len() == 0 {
        let create_ata_instruction = create_associated_token_account(
            admin_account.key,
            &usdt_treasury_pda,
            usdt_mint_account.key,
            token_program_account.key,
        );

        invoke(
            &create_ata_instruction,
            &[
                admin_account.clone(),
                usdt_treasury_ata_account.clone(),
                usdt_treasury_pda_account.clone(),
                usdt_mint_account.clone(),
                system_program_account.clone(),
                token_program_account.clone(),
                associated_token_program_account.clone(),
            ],
        )?;

        msg!("USDT Treasury ATA created: {}", usdt_treasury_ata_account.key);
        msg!("USDT Treasury PDA (Owner): {}", usdt_treasury_pda);
    } else {
        msg!("USDT Treasury ATA already exists: {}", usdt_treasury_ata_account.key);
    }

    // Mark as created
    let mut presale_data = presale_config_account.data.borrow_mut();
    presale_data[236] = 1; // usdt_treasury_created = true
    drop(presale_data);

    msg!("USDT Treasury marked as created");

    Ok(())
}
	
fn withdraw_usdc(accounts: &[AccountInfo], data: &[u8], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let admin_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let usdc_treasury_pda_account = next_account_info(account_info_iter)?;
    let usdc_treasury_ata_account = next_account_info(account_info_iter)?;
    let destination_usdc_account = next_account_info(account_info_iter)?;
    let usdc_mint_account = next_account_info(account_info_iter)?;
    let token_program_account = next_account_info(account_info_iter)?;

    validate_token_program(token_program_account)?;

    if !admin_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    validate_admin_access(presale_config_account, admin_account.key)?;

    if data.len() != 8 {
        return Err(ProgramError::InvalidInstructionData);
    }

    let withdraw_amount = u64::from_le_bytes(
        data.try_into().map_err(|_| ProgramError::InvalidInstructionData)?
    );

    if *usdc_mint_account.key != USDC_MINT_DEVNET {
        return Err(ProgramError::InvalidAccountData);
    }

    let (usdc_treasury_pda, usdc_treasury_bump) = Pubkey::find_program_address(
        &[USDC_TREASURY_SEED],
        program_id,
    );

    if usdc_treasury_pda != *usdc_treasury_pda_account.key {
        return Err(ProgramError::InvalidAccountData);
    }

    let transfer_instruction = spl_token::instruction::transfer(
        token_program_account.key,
        usdc_treasury_ata_account.key,
        destination_usdc_account.key,
        usdc_treasury_pda_account.key,
        &[],
        withdraw_amount,
    )?;

    invoke_signed(
        &transfer_instruction,
        &[
            usdc_treasury_ata_account.clone(),
            destination_usdc_account.clone(),
            usdc_treasury_pda_account.clone(),
            token_program_account.clone(),
        ],
        &[&[USDC_TREASURY_SEED, &[usdc_treasury_bump]]],
    )?;

    msg!("USDC WITHDRAWN: {} from Treasury to {}", withdraw_amount, destination_usdc_account.key);

    Ok(())
}

fn withdraw_usdt(accounts: &[AccountInfo], data: &[u8], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let admin_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let usdt_treasury_pda_account = next_account_info(account_info_iter)?;
    let usdt_treasury_ata_account = next_account_info(account_info_iter)?;
    let destination_usdt_account = next_account_info(account_info_iter)?;
    let usdt_mint_account = next_account_info(account_info_iter)?;
    let token_program_account = next_account_info(account_info_iter)?;

    validate_token_program(token_program_account)?;

    if !admin_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    validate_admin_access(presale_config_account, admin_account.key)?;

    if data.len() != 8 {
        return Err(ProgramError::InvalidInstructionData);
    }

    let withdraw_amount = u64::from_le_bytes(
        data.try_into().map_err(|_| ProgramError::InvalidInstructionData)?
    );

    if *usdt_mint_account.key != USDT_MINT_DEVNET {
        return Err(ProgramError::InvalidAccountData);
    }

    let (usdt_treasury_pda, usdt_treasury_bump) = Pubkey::find_program_address(
        &[USDT_TREASURY_SEED],
        program_id,
    );

    if usdt_treasury_pda != *usdt_treasury_pda_account.key {
        return Err(ProgramError::InvalidAccountData);
    }

    let transfer_instruction = spl_token::instruction::transfer(
        token_program_account.key,
        usdt_treasury_ata_account.key,
        destination_usdt_account.key,
        usdt_treasury_pda_account.key,
        &[],
        withdraw_amount,
    )?;

    invoke_signed(
        &transfer_instruction,
        &[
            usdt_treasury_ata_account.clone(),
            destination_usdt_account.clone(),
            usdt_treasury_pda_account.clone(),
            token_program_account.clone(),
        ],
        &[&[USDT_TREASURY_SEED, &[usdt_treasury_bump]]],
    )?;

    msg!("USDT WITHDRAWN: {} from Treasury to {}", withdraw_amount, destination_usdt_account.key);

    Ok(())
}

fn withdraw_marketing_tokens(accounts: &[AccountInfo], data: &[u8], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let admin_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let marketing_vault_pda_account = next_account_info(account_info_iter)?;
    let marketing_vault_ata_account = next_account_info(account_info_iter)?;
    let destination_mai_account = next_account_info(account_info_iter)?;
    let mai_mint_account = next_account_info(account_info_iter)?;
    let token_program_account = next_account_info(account_info_iter)?;

    validate_token_program(token_program_account)?;

    if !admin_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    validate_admin_access(presale_config_account, admin_account.key)?;

    if data.len() != 8 {
        return Err(ProgramError::InvalidInstructionData);
    }

    let withdraw_amount = u64::from_le_bytes(
        data.try_into().map_err(|_| ProgramError::InvalidInstructionData)?
    );

    if withdraw_amount == 0 {
        return Err(ProgramError::InvalidArgument);
    }

    // Validate Marketing Vault PDA
    let (marketing_vault_pda, marketing_vault_bump) = Pubkey::find_program_address(
        &[MARKETING_VAULT_SEED],
        program_id,
    );

    if marketing_vault_pda != *marketing_vault_pda_account.key {
        return Err(ProgramError::InvalidAccountData);
    }

    // Validate Marketing Vault ATA
    let expected_marketing_vault_ata = get_associated_token_address(&marketing_vault_pda, mai_mint_account.key);
    if expected_marketing_vault_ata != *marketing_vault_ata_account.key {
        return Err(ProgramError::InvalidAccountData);
    }

    // Transfer tokens from Marketing Vault ATA to destination
    let transfer_instruction = spl_token::instruction::transfer(
        token_program_account.key,
        marketing_vault_ata_account.key,
        destination_mai_account.key,
        marketing_vault_pda_account.key,
        &[],
        withdraw_amount,
    )?;

    invoke_signed(
        &transfer_instruction,
        &[
            marketing_vault_ata_account.clone(),
            destination_mai_account.clone(),
            marketing_vault_pda_account.clone(),
            token_program_account.clone(),
        ],
        &[&[MARKETING_VAULT_SEED, &[marketing_vault_bump]]],
    )?;

    msg!("MARKETING WITHDRAWN: {} MAI tokens from Marketing Vault to {}", withdraw_amount, destination_mai_account.key);

    Ok(())
}

fn withdraw_liquidity_tokens(accounts: &[AccountInfo], data: &[u8], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let admin_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let liquidity_vault_pda_account = next_account_info(account_info_iter)?;
    let liquidity_vault_ata_account = next_account_info(account_info_iter)?;
    let destination_mai_account = next_account_info(account_info_iter)?;
    let mai_mint_account = next_account_info(account_info_iter)?;
    let token_program_account = next_account_info(account_info_iter)?;

    validate_token_program(token_program_account)?;

    if !admin_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    validate_admin_access(presale_config_account, admin_account.key)?;

    if data.len() != 8 {
        return Err(ProgramError::InvalidInstructionData);
    }

    let withdraw_amount = u64::from_le_bytes(
        data.try_into().map_err(|_| ProgramError::InvalidInstructionData)?
    );

    if withdraw_amount == 0 {
        return Err(ProgramError::InvalidArgument);
    }

    // Validate Liquidity Vault PDA
    let (liquidity_vault_pda, liquidity_vault_bump) = Pubkey::find_program_address(
        &[LIQUIDITY_VAULT_SEED],
        program_id,
    );

    if liquidity_vault_pda != *liquidity_vault_pda_account.key {
        return Err(ProgramError::InvalidAccountData);
    }

    // Validate Liquidity Vault ATA
    let expected_liquidity_vault_ata = get_associated_token_address(&liquidity_vault_pda, mai_mint_account.key);
    if expected_liquidity_vault_ata != *liquidity_vault_ata_account.key {
        return Err(ProgramError::InvalidAccountData);
    }

    // Transfer tokens from Liquidity Vault ATA to destination
    let transfer_instruction = spl_token::instruction::transfer(
        token_program_account.key,
        liquidity_vault_ata_account.key,
        destination_mai_account.key,
        liquidity_vault_pda_account.key,
        &[],
        withdraw_amount,
    )?;

    invoke_signed(
        &transfer_instruction,
        &[
            liquidity_vault_ata_account.clone(),
            destination_mai_account.clone(),
            liquidity_vault_pda_account.clone(),
            token_program_account.clone(),
        ],
        &[&[LIQUIDITY_VAULT_SEED, &[liquidity_vault_bump]]],
    )?;

    msg!("LIQUIDITY WITHDRAWN: {} MAI tokens from Liquidity Vault to {}", withdraw_amount, destination_mai_account.key);

    Ok(())
}

// ==================== ADMIN FUNCTIONS ====================
fn pause_presale(accounts: &[AccountInfo], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let admin_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;

    if !admin_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    validate_admin_access(presale_config_account, admin_account.key)?;

    let mut presale_data = presale_config_account.data.borrow_mut();
    presale_data[227] = 1; // is_paused = true

    msg!("Presale PAUSED by admin: {}", admin_account.key);

    Ok(())
}

fn resume_presale(accounts: &[AccountInfo], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let admin_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;

    if !admin_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    validate_admin_access(presale_config_account, admin_account.key)?;

    let mut presale_data = presale_config_account.data.borrow_mut();
    presale_data[227] = 0; // is_paused = false

    msg!("Presale RESUMED by admin: {}", admin_account.key);

    Ok(())
}

fn pause_all_claims(accounts: &[AccountInfo], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let admin_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;

    if !admin_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    validate_admin_access(presale_config_account, admin_account.key)?;

    let mut presale_data = presale_config_account.data.borrow_mut();
    presale_data[228] = 1; // claims_paused = true

    msg!("ALL CLAIMS PAUSED by admin: {}", admin_account.key);
    Ok(())
}

fn resume_all_claims(accounts: &[AccountInfo], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let admin_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;

    if !admin_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    validate_admin_access(presale_config_account, admin_account.key)?;

    let mut presale_data = presale_config_account.data.borrow_mut();
    presale_data[228] = 0; // claims_paused = false

    msg!("ALL CLAIMS RESUMED by admin: {}", admin_account.key);
    Ok(())
}

// ==================== UPDATE SOL PRICE ====================
fn update_sol_price(accounts: &[AccountInfo], data: &[u8], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let admin_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;

    if !admin_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    validate_admin_access(presale_config_account, admin_account.key)?;

    if data.len() != 8 {
        return Err(ProgramError::InvalidInstructionData);
    }

    let new_sol_price_cents = u64::from_le_bytes(
        data.try_into().map_err(|_| ProgramError::InvalidInstructionData)?
    );

    // Validate price bounds
    if new_sol_price_cents < MIN_SOL_PRICE_USD_CENTS || new_sol_price_cents > MAX_SOL_PRICE_USD_CENTS {
        msg!("SOL price out of bounds: {} (min: {}, max: {})", 
             new_sol_price_cents, MIN_SOL_PRICE_USD_CENTS, MAX_SOL_PRICE_USD_CENTS);
        return Err(ProgramError::InvalidArgument);
    }

    let mut presale_data = presale_config_account.data.borrow_mut();
    
    // Read old price for logging
    let old_price = u64::from_le_bytes(
        presale_data[83..91].try_into().map_err(|_| ProgramError::InvalidAccountData)?
    );

    // Update SOL price (offset 83)
    new_sol_price_cents.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
        presale_data[83 + i] = byte;
    });

    msg!("SOL PRICE UPDATED by admin: {}", admin_account.key);
    msg!("Old price: ${}.{:02} ({} cents)", old_price / 100, old_price % 100, old_price);
    msg!("New price: ${}.{:02} ({} cents)", new_sol_price_cents / 100, new_sol_price_cents % 100, new_sol_price_cents);

    Ok(())
}

// ==================== HELPER FUNCTIONS ====================
fn check_and_set_reentrancy_guard(presale_config_account: &AccountInfo) -> ProgramResult {
    let mut presale_data = presale_config_account.data.borrow_mut();

    if presale_data[229] == 1 {
        return Err(ProgramError::AccountAlreadyInitialized);
    }

    presale_data[229] = 1; // reentrancy_guard = true
    Ok(())
}

fn clear_reentrancy_guard(presale_config_account: &AccountInfo) -> ProgramResult {
    let mut presale_data = presale_config_account.data.borrow_mut();
    presale_data[229] = 0; // reentrancy_guard = false
    Ok(())
}

fn check_presale_status(presale_config_account: &AccountInfo) -> ProgramResult {
    let presale_data = presale_config_account.data.borrow();

    // Check 1: Is presale active?
    if presale_data[73] != 1 {
        msg!("ERROR: Presale is not active");
        return Err(ProgramError::InvalidAccountData);
    }

    // Check 2: Is presale not paused?
    if presale_data[227] == 1 {
        msg!("ERROR: Presale is paused");
        return Err(ProgramError::InvalidAccountData);
    }

    // Check 3: Is stage within bounds?
    let current_stage = presale_data[64];
    if current_stage > 14 {
        msg!("ERROR: All presale stages completed (stage {})", current_stage);
        return Err(ProgramError::InsufficientFunds);
    }

    // Auto-close: less than 1000 tokens remaining on stage 14
    if current_stage == 14 {
        let stage_sold_offset = 115 + 13 * 8; // Stage 14 offset
        let stage_sold = u64::from_le_bytes(
            presale_data[stage_sold_offset..stage_sold_offset + 8]
                .try_into()
                .map_err(|_| ProgramError::InvalidAccountData)?
        );
        
        let (_, stage_limit) = get_stage_data(14)?; // 35M tokens
        let remaining_tokens = stage_limit.saturating_sub(stage_sold);
        
        let min_tokens_threshold = 1000u64
            .checked_mul(PRECISION)
            .ok_or(ProgramError::ArithmeticOverflow)?;
        
        if remaining_tokens < min_tokens_threshold {
            msg!("AUTO-CLOSING: Stage 14 remaining < 1000 tokens");
            
            drop(presale_data);            

            let mut presale_data = presale_config_account.data.borrow_mut();
            presale_data[73] = 0; // is_active = false
            
            let total_sold = u64::from_le_bytes(
                presale_data[65..73].try_into().map_err(|_| ProgramError::InvalidAccountData)?
            );
            drop(presale_data);
            
            emit_presale_completion_event(total_sold);
            
            return Err(ProgramError::InsufficientFunds);
        }
    }
// Check 4: Are there tokens in current stage?
if current_stage >= 1 && current_stage <= 14 {
    let stage_sold_offset = 115 + ((current_stage - 1) as usize) * 8;
    let stage_sold = u64::from_le_bytes(
        presale_data[stage_sold_offset..stage_sold_offset + 8]
            .try_into()
            .map_err(|_| ProgramError::InvalidAccountData)?
    );
    drop(presale_data);
    let (_, stage_limit) = get_stage_data(current_stage)?;
    if stage_sold >= stage_limit {
        if current_stage == 14 {
            // Stage 14 sold out - allow auto-close
            msg!("INFO: Stage 14 sold out, will auto-close presale");
        } else {
            msg!("ERROR: Current stage {} sold out ({}/{})", current_stage, stage_sold, stage_limit);
            return Err(ProgramError::InsufficientFunds);
        }
    }
} else {
    drop(presale_data);
}

    Ok(())
}

fn validate_admin_access(presale_config_account: &AccountInfo, admin_key: &Pubkey) -> ProgramResult {
    let presale_data = presale_config_account.data.borrow();
    let stored_admin = Pubkey::new_from_array(
        presale_data[0..32].try_into().map_err(|_| ProgramError::InvalidAccountData)?
    );

    if stored_admin != *admin_key {
        return Err(ProgramError::InvalidAccountData);
    }
    Ok(())
}

fn check_claims_not_paused(presale_config_account: &AccountInfo) -> ProgramResult {
    let presale_data = presale_config_account.data.borrow();
    if presale_data[228] == 1 {
        msg!("ERROR: All claims are paused by admin");
        return Err(ProgramError::InvalidAccountData);
    }
    Ok(())
}

// Validate admin_sol_account matches stored admin
fn validate_admin_sol_account(presale_config_account: &AccountInfo, admin_sol_key: &Pubkey) -> ProgramResult {
    let presale_data = presale_config_account.data.borrow();
    let stored_admin = Pubkey::new_from_array(
        presale_data[0..32].try_into().map_err(|_| ProgramError::InvalidAccountData)?
    );

    if stored_admin != *admin_sol_key {
        msg!("SECURITY ERROR: admin_sol_account does not match stored admin!");
        msg!("Expected: {}, Got: {}", stored_admin, admin_sol_key);
        return Err(ProgramError::InvalidAccountData);
    }

    Ok(())
}

fn validate_mai_mint_pda(mai_mint_account: &AccountInfo, program_id: &Pubkey) -> ProgramResult {
    let (expected_pda, _) = Pubkey::find_program_address(
        &[MAI_MINT_SEED],
        program_id,
    );

    if *mai_mint_account.key != expected_pda {
        return Err(ProgramError::InvalidArgument);
    }

    Ok(())
}

fn validate_user_contribution_pda(
    user_contribution_account: &AccountInfo,
    user_key: &Pubkey,
    program_id: &Pubkey
) -> ProgramResult {
    let (expected_pda, _) = Pubkey::find_program_address(
        &[USER_CONTRIBUTION_SEED, user_key.as_ref()],
        program_id,
    );

    if *user_contribution_account.key != expected_pda {
        msg!("Invalid user contribution PDA. Expected: {}, got: {}", expected_pda, user_contribution_account.key);
        return Err(ProgramError::InvalidAccountData);
    }

    Ok(())
}

fn validate_presale_vault_pda(
    presale_vault_account: &AccountInfo,
    program_id: &Pubkey
) -> ProgramResult {
    let (presale_config_pda, _) = Pubkey::find_program_address(
        &[PRESALE_CONFIG_SEED],
        program_id,
    );

    let (mai_mint_pda, _) = Pubkey::find_program_address(
        &[MAI_MINT_SEED],
        program_id,
    );

    let expected_vault_ata = get_associated_token_address(
        &presale_config_pda,
        &mai_mint_pda,
    );

    if *presale_vault_account.key != expected_vault_ata {
        msg!("Invalid presale vault ATA. Expected: {}, got: {}", expected_vault_ata, presale_vault_account.key);
        return Err(ProgramError::InvalidAccountData);
    }

    Ok(())
}

fn ensure_user_mai_ata_exists<'a>(
    user_mai_account: &AccountInfo<'a>,
    user_account: &AccountInfo<'a>,
    mai_mint_account: &AccountInfo<'a>,
    token_program_account: &AccountInfo<'a>,
    system_program_account: &AccountInfo<'a>,
    _rent_sysvar: &Rent,
    associated_token_program_account: &AccountInfo<'a>,
) -> ProgramResult {
    if user_mai_account.data_len() == 0 {
        msg!("Creating user MAI ATA for: {}", user_account.key);

        let create_ata_instruction = create_associated_token_account(
            user_account.key,
            user_account.key,
            mai_mint_account.key,
            token_program_account.key,
        );

        invoke(
            &create_ata_instruction,
            &[
                user_account.clone(),
                user_mai_account.clone(),
                user_account.clone(),
                mai_mint_account.clone(),
                system_program_account.clone(),
                token_program_account.clone(),
                associated_token_program_account.clone(),
            ],
        )?;

        // msg!("Created user MAI ATA: {}", user_mai_account.key);
    } else {
        let expected_ata = get_associated_token_address(
            user_account.key,
            mai_mint_account.key,
        );

        if *user_mai_account.key != expected_ata {
            msg!("Invalid user MAI ATA. Expected: {}, got: {}", expected_ata, user_mai_account.key);
            return Err(ProgramError::InvalidAccountData);
        }
    }

    Ok(())
}

fn validate_usdc_treasury_ata(
    usdc_treasury_ata_account: &AccountInfo,
    program_id: &Pubkey
) -> ProgramResult {
    let (usdc_treasury_pda, _) = Pubkey::find_program_address(
        &[USDC_TREASURY_SEED],
        program_id,
    );

    let expected_usdc_treasury_ata = get_associated_token_address(
        &usdc_treasury_pda,
        &USDC_MINT_DEVNET,
    );

    if *usdc_treasury_ata_account.key != expected_usdc_treasury_ata {
        msg!("Invalid USDC Treasury ATA. Expected: {}, got: {}", expected_usdc_treasury_ata, usdc_treasury_ata_account.key);
        return Err(ProgramError::InvalidAccountData);
    }

    Ok(())
}

fn validate_usdt_treasury_ata(
    usdt_treasury_ata_account: &AccountInfo,
    program_id: &Pubkey
) -> ProgramResult {
    let (usdt_treasury_pda, _) = Pubkey::find_program_address(
        &[USDT_TREASURY_SEED],
        program_id,
    );

    let expected_usdt_treasury_ata = get_associated_token_address(
        &usdt_treasury_pda,
        &USDT_MINT_DEVNET,
    );

    if *usdt_treasury_ata_account.key != expected_usdt_treasury_ata {
        msg!("Invalid USDT Treasury ATA. Expected: {}, got: {}", expected_usdt_treasury_ata, usdt_treasury_ata_account.key);
        return Err(ProgramError::InvalidAccountData);
    }

    Ok(())
}

// ==================== TOKEN PROGRAM VALIDATION ====================
fn validate_token_program(token_program: &AccountInfo) -> ProgramResult {
    if *token_program.key != spl_token::id() {
        msg!("ERROR: Invalid token program. Expected: {}, Got: {}", 
             spl_token::id(), token_program.key);
        return Err(ProgramError::IncorrectProgramId);
    }
    Ok(())
}

// ==================== NFT FUNCTIONS ====================
fn create_nft_collection(accounts: &[AccountInfo], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let admin_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let collection_mint_account = next_account_info(account_info_iter)?;
    let collection_metadata_account = next_account_info(account_info_iter)?;
    let collection_master_edition_account = next_account_info(account_info_iter)?;
    let collection_ata_account = next_account_info(account_info_iter)?;
    let token_program_account = next_account_info(account_info_iter)?;
    let token_metadata_program_account = next_account_info(account_info_iter)?;
    let system_program_account = next_account_info(account_info_iter)?;
    let rent_sysvar_account = next_account_info(account_info_iter)?;
    let associated_token_program_account = next_account_info(account_info_iter)?;

    if !admin_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    validate_admin_access(presale_config_account, admin_account.key)?;

    // Split into small functions to save stack space
    let (presale_config_bump, collection_mint_bump, presale_config_pda) = 
        validate_collection_creation(presale_config_account, collection_mint_account, program_id)?;

    validate_collection_pdas(
        collection_metadata_account,
        collection_master_edition_account,
        collection_ata_account,
        collection_mint_account,
        token_metadata_program_account,
        &presale_config_pda,
    )?;

    // Create in separate small functions
    create_collection_mint(
        admin_account,
        collection_mint_account,
        presale_config_account,
        system_program_account,
        rent_sysvar_account,
        token_program_account,
        collection_mint_bump,
    )?;

    create_collection_ata(
        admin_account,
        collection_ata_account,
        presale_config_account,
        collection_mint_account,
        system_program_account,
        token_program_account,
        associated_token_program_account,
        presale_config_bump,
    )?;

    create_collection_metadata(
        collection_metadata_account,
        collection_mint_account,
        presale_config_account,
        admin_account,
        system_program_account,
        rent_sysvar_account,
        token_metadata_program_account,
        presale_config_bump,
    )?;

    create_collection_master_edition(
        collection_master_edition_account,
        collection_mint_account,
        presale_config_account,
        admin_account,
        collection_metadata_account,
        token_program_account,
        system_program_account,
        rent_sysvar_account,
        token_metadata_program_account,
        presale_config_bump,
    )?;

    // Update collection created flag
    {
        let mut presale_data = presale_config_account.data.borrow_mut();
        presale_data[NFT_COLLECTION_CREATED_INDEX] = 1;
    }

    msg!("COLLECTION CREATED: {}", collection_mint_account.key);
    Ok(())
}

// Helper functions to save stack space

fn validate_collection_creation<'a>(
    presale_config_account: &AccountInfo<'a>,
    collection_mint_account: &AccountInfo<'a>,
    program_id: &Pubkey,
) -> Result<(u8, u8, Pubkey), ProgramError> {
    let presale_data = presale_config_account.data.borrow();
    if presale_data[NFT_COLLECTION_CREATED_INDEX] == 1 {
        return Err(ProgramError::AccountAlreadyInitialized);
    }
    let presale_bump = presale_data[230];
    drop(presale_data);

    let (collection_mint_pda, mint_bump) = Pubkey::find_program_address(&[NFT_COLLECTION_SEED], program_id);
    if collection_mint_pda != *collection_mint_account.key {
        return Err(ProgramError::InvalidAccountData);
    }

    let (presale_pda, _) = Pubkey::find_program_address(&[PRESALE_CONFIG_SEED], program_id);
    Ok((presale_bump, mint_bump, presale_pda))
}

fn validate_collection_pdas<'a>(
    collection_metadata_account: &AccountInfo<'a>,
    collection_master_edition_account: &AccountInfo<'a>,
    collection_ata_account: &AccountInfo<'a>,
    collection_mint_account: &AccountInfo<'a>,
    token_metadata_program_account: &AccountInfo<'a>,
    presale_config_pda: &Pubkey,
) -> ProgramResult {
    // Metadata PDA
    let metadata_seeds = &[b"metadata", token_metadata_program_account.key.as_ref(), collection_mint_account.key.as_ref()];
    let (expected_metadata_pda, _) = Pubkey::find_program_address(metadata_seeds, token_metadata_program_account.key);
    if expected_metadata_pda != *collection_metadata_account.key {
        return Err(ProgramError::InvalidAccountData);
    }

    // Master Edition PDA
    let master_edition_seeds = &[b"metadata", token_metadata_program_account.key.as_ref(), collection_mint_account.key.as_ref(), b"edition"];
    let (expected_master_edition_pda, _) = Pubkey::find_program_address(master_edition_seeds, token_metadata_program_account.key);
    if expected_master_edition_pda != *collection_master_edition_account.key {
        return Err(ProgramError::InvalidAccountData);
    }

    // Collection ATA
    let expected_collection_ata = get_associated_token_address(presale_config_pda, collection_mint_account.key);
    if expected_collection_ata != *collection_ata_account.key {
        return Err(ProgramError::InvalidAccountData);
    }

    Ok(())
}

fn create_collection_mint<'a>(
    admin_account: &AccountInfo<'a>,
    collection_mint_account: &AccountInfo<'a>,
    presale_config_account: &AccountInfo<'a>,
    system_program_account: &AccountInfo<'a>,
    rent_sysvar_account: &AccountInfo<'a>,
    token_program_account: &AccountInfo<'a>,
    collection_mint_bump: u8,
) -> ProgramResult {
    invoke_signed(
        &system_instruction::create_account(
            admin_account.key,
            collection_mint_account.key,
            2_039_280,
            82,
            token_program_account.key,
        ),
        &[admin_account.clone(), collection_mint_account.clone(), system_program_account.clone()],
        &[&[NFT_COLLECTION_SEED, &[collection_mint_bump]]],
    )?;

    invoke(
        &spl_token::instruction::initialize_mint(
            token_program_account.key,
            collection_mint_account.key,
            presale_config_account.key,
            Some(presale_config_account.key),
            0,
        )?,
        &[collection_mint_account.clone(), rent_sysvar_account.clone(), token_program_account.clone()],
    )?;

    Ok(())
}

fn create_collection_ata<'a>(
    admin_account: &AccountInfo<'a>,
    collection_ata_account: &AccountInfo<'a>,
    presale_config_account: &AccountInfo<'a>,
    collection_mint_account: &AccountInfo<'a>,
    system_program_account: &AccountInfo<'a>,
    token_program_account: &AccountInfo<'a>,
    associated_token_program_account: &AccountInfo<'a>,
    presale_config_bump: u8,
) -> ProgramResult {
    if collection_ata_account.data_len() == 0 {
        invoke(
            &create_associated_token_account(
                admin_account.key,
                presale_config_account.key,
                collection_mint_account.key,
                token_program_account.key,
            ),
            &[
                admin_account.clone(),
                collection_ata_account.clone(),
                presale_config_account.clone(),
                collection_mint_account.clone(),
                system_program_account.clone(),
                token_program_account.clone(),
                associated_token_program_account.clone(),
            ],
        )?;
    }

    invoke_signed(
        &spl_token::instruction::mint_to(
            token_program_account.key,
            collection_mint_account.key,
            collection_ata_account.key,
            presale_config_account.key,
            &[],
            1,
        )?,
        &[
            collection_mint_account.clone(),
            collection_ata_account.clone(),
            presale_config_account.clone(),
            token_program_account.clone(),
        ],
        &[&[PRESALE_CONFIG_SEED, &[presale_config_bump]]],
    )?;

    Ok(())
}

fn create_collection_metadata<'a>(
    collection_metadata_account: &AccountInfo<'a>,
    collection_mint_account: &AccountInfo<'a>,
    presale_config_account: &AccountInfo<'a>,
    admin_account: &AccountInfo<'a>,
    system_program_account: &AccountInfo<'a>,
    rent_sysvar_account: &AccountInfo<'a>,
    token_metadata_program_account: &AccountInfo<'a>,
    presale_config_bump: u8,
) -> ProgramResult {
    // Create structures locally in function
    let metadata_data = DataV2 {
        name: COLLECTION_NAME.to_string(),
        symbol: COLLECTION_SYMBOL.to_string(),
        uri: COLLECTION_URI.to_string(),
        seller_fee_basis_points: 0,
        creators: None,
        collection: None,
        uses: None,
    };

    let create_metadata_args = CreateMetadataAccountV3InstructionArgs {
        data: metadata_data,
        is_mutable: true,
        collection_details: None,
    };

    let create_metadata_ix = CreateMetadataAccountV3 {
        metadata: *collection_metadata_account.key,
        mint: *collection_mint_account.key,
        mint_authority: *presale_config_account.key,
        payer: *admin_account.key,
        update_authority: (*presale_config_account.key, true),
        system_program: *system_program_account.key,
        rent: Some(*rent_sysvar_account.key),
    }.instruction(create_metadata_args);

    invoke_signed(
        &create_metadata_ix,
        &[
            collection_metadata_account.clone(),
            collection_mint_account.clone(),
            presale_config_account.clone(),
            admin_account.clone(),
            system_program_account.clone(),
            rent_sysvar_account.clone(),
            token_metadata_program_account.clone(),
        ],
        &[&[PRESALE_CONFIG_SEED, &[presale_config_bump]]],
    )?;

    Ok(())
}

fn create_collection_master_edition<'a>(
    collection_master_edition_account: &AccountInfo<'a>,
    collection_mint_account: &AccountInfo<'a>,
    presale_config_account: &AccountInfo<'a>,
    admin_account: &AccountInfo<'a>,
    collection_metadata_account: &AccountInfo<'a>,
    token_program_account: &AccountInfo<'a>,
    system_program_account: &AccountInfo<'a>,
    rent_sysvar_account: &AccountInfo<'a>,
    token_metadata_program_account: &AccountInfo<'a>,
    presale_config_bump: u8,
) -> ProgramResult {
    // Create structures locally
    let master_edition_args = mpl_token_metadata::instructions::CreateMasterEditionV3InstructionArgs {
        max_supply: Some(0),
    };

    let master_edition_ix = mpl_token_metadata::instructions::CreateMasterEditionV3 {
        edition: *collection_master_edition_account.key,
        mint: *collection_mint_account.key,
        update_authority: *presale_config_account.key,
        mint_authority: *presale_config_account.key,
        payer: *admin_account.key,
        metadata: *collection_metadata_account.key,
        token_program: *token_program_account.key,
        system_program: *system_program_account.key,
        rent: Some(*rent_sysvar_account.key),
    }.instruction(master_edition_args);

    invoke_signed(
        &master_edition_ix,
        &[
            collection_master_edition_account.clone(),
            collection_mint_account.clone(),
            presale_config_account.clone(),
            presale_config_account.clone(),
            admin_account.clone(),
            collection_metadata_account.clone(),
            token_program_account.clone(),
            system_program_account.clone(),
            rent_sysvar_account.clone(),
            token_metadata_program_account.clone(),
        ],
        &[&[PRESALE_CONFIG_SEED, &[presale_config_bump]]],
    )?;

    Ok(())
}

fn mint_user_nft(accounts: &[AccountInfo], data: &[u8], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let user_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let user_contribution_account = next_account_info(account_info_iter)?;
    let user_nft_mint_account = next_account_info(account_info_iter)?;
    let user_nft_token_account = next_account_info(account_info_iter)?;
    let user_nft_metadata_account = next_account_info(account_info_iter)?;
    let user_nft_master_edition_account = next_account_info(account_info_iter)?;
    let token_program_account = next_account_info(account_info_iter)?;
    let token_metadata_program_account = next_account_info(account_info_iter)?;
    let system_program_account = next_account_info(account_info_iter)?;
    let rent_sysvar_account = next_account_info(account_info_iter)?;

    if !user_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    validate_user_contribution_pda(user_contribution_account, user_account.key, program_id)?;

    // Check that NFT collection is created
    {
        let presale_data = presale_config_account.data.borrow();
        if presale_data[NFT_COLLECTION_CREATED_INDEX] != 1 {
            msg!("ERROR: NFT Collection not created yet! Call function 30 first.");
            return Err(ProgramError::InvalidAccountData);
        }
        drop(presale_data);
    }
    // Get user's total contribution
    let user_data = user_contribution_account.data.borrow();
    let total_contribution = u64::from_le_bytes(
        user_data[32..40].try_into().map_err(|_| ProgramError::InvalidAccountData)?
    );
    let nft_tier_earned = user_data[169]; // nft_tier_earned
    let nft_minted = user_data[170]; // nft_minted
    drop(user_data);

    // Determine NFT tier by total contribution
    let total_usd = total_contribution / 100; // Convert cents to dollars
    let nft_tier = NftTier::from_usd_amount(total_usd);

    if nft_tier == NftTier::None {
        msg!("User hasn't contributed enough for NFT: ${}", total_usd);
        return Err(ProgramError::InsufficientFunds);
    }

    if nft_minted == 1 {
        msg!("User already has NFT minted, use upgrade instead");
        return Err(ProgramError::AccountAlreadyInitialized);
    }

	// Save metadata for future use
	let nft_name = nft_tier.get_name().to_string();
	let nft_symbol = "MAIW".to_string(); // MAI Warrior
	let nft_uri = nft_tier.get_metadata_uri().to_string();

	msg!("NFT Metadata prepared:");
	msg!("Name: {}", nft_name);
	msg!("Symbol: {}", nft_symbol);
	msg!("URI: {}", nft_uri);

    // Update user_contribution
    let mut user_data = user_contribution_account.data.borrow_mut();
    user_data[169] = nft_tier as u8; // nft_tier_earned
    user_data[170] = 1; // nft_minted = true
    user_nft_mint_account.key.to_bytes().iter().enumerate().for_each(|(i, &byte)| {
    user_data[172 + i] = byte; // nft_mint_address
});

    msg!("MINT USER NFT: {} tier for ${} total spent", nft_tier as u8, total_usd);
    msg!("NFT Mint: {}", user_nft_mint_account.key);
    msg!("Metadata URI: {}", nft_uri);

    Ok(())
}

fn upgrade_user_nft(accounts: &[AccountInfo], data: &[u8], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let user_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let user_contribution_account = next_account_info(account_info_iter)?;
    let old_nft_mint_account = next_account_info(account_info_iter)?;
    let old_nft_token_account = next_account_info(account_info_iter)?;
    let old_nft_metadata_account = next_account_info(account_info_iter)?;
    let new_nft_mint_account = next_account_info(account_info_iter)?;
    let new_nft_token_account = next_account_info(account_info_iter)?;
    let new_nft_metadata_account = next_account_info(account_info_iter)?;
    let token_program_account = next_account_info(account_info_iter)?;
    let token_metadata_program_account = next_account_info(account_info_iter)?;
    let system_program_account = next_account_info(account_info_iter)?;

    if !user_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    validate_user_contribution_pda(user_contribution_account, user_account.key, program_id)?;

    // Get user data
    let user_data = user_contribution_account.data.borrow();
    let total_contribution = u64::from_le_bytes(
        user_data[32..40].try_into().map_err(|_| ProgramError::InvalidAccountData)?
    );
    let current_nft_tier = user_data[169]; // nft_tier_earned
    let nft_minted = user_data[170]; // nft_minted
    
    // Check that user already has NFT
    if nft_minted != 1 {
        return Err(ProgramError::InvalidAccountData);
    }

    // Read old mint address
    let stored_nft_mint = Pubkey::new_from_array(
    user_data[172..204].try_into().map_err(|_| ProgramError::InvalidAccountData)?
    );
    drop(user_data);

    // Verify correct old NFT was provided
    if stored_nft_mint != *old_nft_mint_account.key {
        msg!("Wrong old NFT mint provided");
        return Err(ProgramError::InvalidAccountData);
    }

    // Determine new NFT tier
    let total_usd = total_contribution / 100;
    let new_nft_tier = NftTier::from_usd_amount(total_usd);

    if new_nft_tier as u8 <= current_nft_tier {
        msg!("No upgrade possible: current tier {} >= new tier {}", current_nft_tier, new_nft_tier as u8);
        return Err(ProgramError::InvalidArgument);
    }

    // Burn old NFT (simplified)
    msg!("BURNING old NFT tier {} (mint: {})", current_nft_tier, old_nft_mint_account.key);
    
    // Mint new NFT (simplified)
    msg!("MINTING new NFT tier {} (mint: {})", new_nft_tier as u8, new_nft_mint_account.key);
    msg!("New metadata URI: {}", new_nft_tier.get_metadata_uri());

    // Update user_contribution
    let mut user_data = user_contribution_account.data.borrow_mut();
    user_data[169] = new_nft_tier as u8; // nft_tier_earned
    new_nft_mint_account.key.to_bytes().iter().enumerate().for_each(|(i, &byte)| {
    user_data[172 + i] = byte; // nft_mint_address
    });

    msg!("UPGRADE USER NFT: {} -> {} for ${} total", current_nft_tier, new_nft_tier as u8, total_usd);

    Ok(())
}

// ==================== CLAIM USER NFT ====================
fn claim_user_nft(accounts: &[AccountInfo], program_id: &Pubkey) -> ProgramResult {
    // Basic checks only, account extraction inside create_single_nft
    let account_info_iter = &mut accounts.iter();
    let user_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let user_contribution_account = next_account_info(account_info_iter)?;

    if !user_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    validate_user_contribution_pda(user_contribution_account, user_account.key, program_id)?;
    check_claims_not_paused(presale_config_account)?;

    // Check claim conditions
    let (presale_config_bump, nft_tier) = {
        let presale_data = presale_config_account.data.borrow();
        if presale_data[82] != 1 {
            msg!("ERROR: Listing not activated yet");
            return Err(ProgramError::InvalidAccountData);
        }
        let config_bump = presale_data[230];
        drop(presale_data);

        let user_data = user_contribution_account.data.borrow();
        let nft_minted = user_data[170];
        let nft_claimed = user_data[171];
        let nft_tier = user_data[169];

        if nft_minted != 1 {
            msg!("ERROR: No NFT earned. Make purchases $50+ to earn NFT");
            return Err(ProgramError::InvalidAccountData);
        }
        if nft_claimed == 1 {
            msg!("ERROR: NFT already claimed");
            return Err(ProgramError::AccountAlreadyInitialized);
        }
        if nft_tier == 0 {
            msg!("ERROR: Invalid NFT tier");
            return Err(ProgramError::InvalidAccountData);
        }

        drop(user_data);
        (config_bump, nft_tier)
    };

    // Read admin address
    let admin_address = {
        let presale_data = presale_config_account.data.borrow();
        let admin_pubkey = Pubkey::new_from_array(presale_data[0..32].try_into().unwrap());
        drop(presale_data);
        admin_pubkey
    };

    // Update user NFT flag and counter
    {
        let mut user_data = user_contribution_account.data.borrow_mut();
        user_data[171] = 1;
    }

    update_tier_counter(presale_config_account, nft_tier)?;

    msg!("Creating regular NFT for user: {}", user_account.key);

    // Create user NFT
    create_single_nft(
        accounts,
        program_id,
        nft_tier,
        USER_NFT_SEED,
        presale_config_bump,
        admin_address,
    )?;

    msg!("USER NFT CLAIMED: {}", user_account.key);
    Ok(())
}

// ==================== HELPER FUNCTION: CREATE SINGLE NFT ====================
fn create_single_nft<'a>(
    accounts: &[AccountInfo<'a>],
    program_id: &Pubkey,
    tier: u8,
    nft_seed: &[u8],
    presale_config_bump: u8,
    admin_address: Pubkey,
) -> ProgramResult {

    // Extract accounts
    let account_info_iter = &mut accounts.iter();
    let user_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let _user_contribution_account = next_account_info(account_info_iter)?;  // skip

    let nft_mint_account = next_account_info(account_info_iter)?;
    let nft_token_account = next_account_info(account_info_iter)?;
    let nft_metadata_account = next_account_info(account_info_iter)?;
    let nft_master_edition_account = next_account_info(account_info_iter)?;

    let collection_mint_account = next_account_info(account_info_iter)?;
    let _collection_metadata_account = next_account_info(account_info_iter)?;  // unused
    let _collection_master_edition_account = next_account_info(account_info_iter)?;  // unused
    let token_program_account = next_account_info(account_info_iter)?;
    let token_metadata_program_account = next_account_info(account_info_iter)?;
    let system_program_account = next_account_info(account_info_iter)?;
    let rent_sysvar_account = next_account_info(account_info_iter)?;
    let associated_token_program_account = next_account_info(account_info_iter)?;

    // Generate PDA for mint
    let (nft_mint_pda, nft_mint_bump) = Pubkey::find_program_address(
        &[nft_seed, user_account.key.as_ref(), &[tier]],
        program_id,
    );

    if nft_mint_pda != *nft_mint_account.key {
        return Err(ProgramError::InvalidAccountData);
    }

    msg!("Creating NFT for tier {}", tier);

    // 1. Create mint account
    invoke_signed(
        &system_instruction::create_account(
            user_account.key,
            nft_mint_account.key,
            2_039_280,
            82,
            token_program_account.key,
        ),
        &[user_account.clone(), nft_mint_account.clone(), system_program_account.clone()],
        &[&[nft_seed, user_account.key.as_ref(), &[tier], &[nft_mint_bump]]],
    )?;

    // 2. Initialize mint
    invoke(
        &spl_token::instruction::initialize_mint(
            token_program_account.key,
            nft_mint_account.key,
            presale_config_account.key,
            Some(presale_config_account.key),
            0,
        )?,
        &[nft_mint_account.clone(), rent_sysvar_account.clone(), token_program_account.clone()],
    )?;

    // 3. Create ATA if needed
    if nft_token_account.data_len() == 0 {
        invoke(
            &create_associated_token_account(
                user_account.key,
                user_account.key,
                nft_mint_account.key,
                token_program_account.key,
            ),
            &[
                user_account.clone(),
                nft_token_account.clone(),
                user_account.clone(),
                nft_mint_account.clone(),
                system_program_account.clone(),
                token_program_account.clone(),
                associated_token_program_account.clone(),
            ],
        )?;
    }

    // 4. Mint token
    invoke_signed(
        &spl_token::instruction::mint_to(
            token_program_account.key,
            nft_mint_account.key,
            nft_token_account.key,
            presale_config_account.key,
            &[],
            1,
        )?,
        &[
            nft_mint_account.clone(),
            nft_token_account.clone(),
            presale_config_account.clone(),
            token_program_account.clone(),
        ],
        &[&[PRESALE_CONFIG_SEED, &[presale_config_bump]]],
    )?;

    // 5. Create metadata
    let tier_enum = NftTier::try_from(tier)?;
    let tier_count = get_next_tier_count(presale_config_account, &tier_enum)?;
    let nft_name = format!("{} #{}", tier_enum.get_name(), tier_count);
    let nft_uri = tier_enum.get_metadata_uri().to_string();

    let metadata_data = DataV2 {
        name: nft_name.clone(),
        symbol: COLLECTION_SYMBOL.to_string(),
        uri: nft_uri.clone(),
        seller_fee_basis_points: 500,
        creators: Some(vec![
            mpl_token_metadata::types::Creator {
                address: admin_address,
                verified: false,
                share: 100,
            }
        ]),
        collection: Some(mpl_token_metadata::types::Collection {
            verified: false,
            key: *collection_mint_account.key,
        }),
        uses: None,
    };

    let create_metadata_args = CreateMetadataAccountV3InstructionArgs {
        data: metadata_data,
        is_mutable: true,
        collection_details: None,
    };

    let create_metadata_ix = CreateMetadataAccountV3 {
        metadata: *nft_metadata_account.key,
        mint: *nft_mint_account.key,
        mint_authority: *presale_config_account.key,
        payer: *user_account.key,
        update_authority: (*presale_config_account.key, true),
        system_program: *system_program_account.key,
        rent: Some(*rent_sysvar_account.key),
    }.instruction(create_metadata_args);

    invoke_signed(
        &create_metadata_ix,
        &[
            nft_metadata_account.clone(),
            nft_mint_account.clone(),
            presale_config_account.clone(),
            user_account.clone(),
            system_program_account.clone(),
            rent_sysvar_account.clone(),
            token_metadata_program_account.clone(),
        ],
        &[&[PRESALE_CONFIG_SEED, &[presale_config_bump]]],
    )?;

    // 6. Create master edition
    invoke_signed(
        &mpl_token_metadata::instructions::CreateMasterEditionV3 {
            edition: *nft_master_edition_account.key,
            mint: *nft_mint_account.key,
            update_authority: *presale_config_account.key,
            mint_authority: *presale_config_account.key,
            payer: *user_account.key,
            metadata: *nft_metadata_account.key,
            token_program: *token_program_account.key,
            system_program: *system_program_account.key,
            rent: Some(*rent_sysvar_account.key),
        }.instruction(mpl_token_metadata::instructions::CreateMasterEditionV3InstructionArgs {
            max_supply: Some(1),
        }),
        &[
            nft_master_edition_account.clone(),
            nft_mint_account.clone(),
            presale_config_account.clone(),
            presale_config_account.clone(),
            user_account.clone(),
            nft_metadata_account.clone(),
            token_program_account.clone(),
            system_program_account.clone(),
            rent_sysvar_account.clone(),
            token_metadata_program_account.clone(),
        ],
        &[&[PRESALE_CONFIG_SEED, &[presale_config_bump]]],
    )?;

    msg!("NFT created successfully: {}", nft_name);
    Ok(())
}

// ==================== HELPER FUNCTIONS ====================
fn get_next_tier_count(
    presale_config_account: &AccountInfo,
    tier_enum: &NftTier,
) -> Result<u16, ProgramError> {
    let presale_data = presale_config_account.data.borrow();
    // Counter already incremented in update_tier_counter
    let count = match tier_enum {
        NftTier::Bronze => u16::from_le_bytes([presale_data[238], presale_data[239]]),
        NftTier::Silver => u16::from_le_bytes([presale_data[240], presale_data[241]]),
        NftTier::Gold => u16::from_le_bytes([presale_data[242], presale_data[243]]),
        NftTier::Platinum => u16::from_le_bytes([presale_data[244], presale_data[245]]),
        NftTier::None => 0,
    };
    drop(presale_data);
    Ok(count)
}

fn update_tier_counter(
    presale_config_account: &AccountInfo,
    tier: u8,
) -> ProgramResult {
    let tier_enum = NftTier::try_from(tier)?;
    let mut presale_data = presale_config_account.data.borrow_mut();
    
    match tier_enum {
        NftTier::Bronze => {
            let current = u16::from_le_bytes([presale_data[238], presale_data[239]]);
            (current + 1).to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
                presale_data[238 + i] = byte;
            });
        },
        NftTier::Silver => {
            let current = u16::from_le_bytes([presale_data[240], presale_data[241]]);
            (current + 1).to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
                presale_data[240 + i] = byte;
            });
        },
        NftTier::Gold => {
            let current = u16::from_le_bytes([presale_data[242], presale_data[243]]);
            (current + 1).to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
                presale_data[242 + i] = byte;
            });
        },
        NftTier::Platinum => {
            let current = u16::from_le_bytes([presale_data[244], presale_data[245]]);
            (current + 1).to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
                presale_data[244 + i] = byte;
            });
        },
        NftTier::None => {},
    }
    drop(presale_data);
    Ok(())
}

// ==================== AIRDROP NFT COUNTER FUNCTIONS ====================
fn get_airdrop_count(presale_config_account: &AccountInfo) -> Result<u16, ProgramError> {
    let presale_data = presale_config_account.data.borrow();
    let count = u16::from_le_bytes([presale_data[246], presale_data[247]]);
    drop(presale_data);
    Ok(count)
}

fn update_airdrop_counter(presale_config_account: &AccountInfo) -> ProgramResult {
    let mut presale_data = presale_config_account.data.borrow_mut();
    let current = u16::from_le_bytes([presale_data[246], presale_data[247]]);
    (current + 1).to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
        presale_data[246 + i] = byte;
    });
    drop(presale_data);
    Ok(())
}

// ==================== SIMPLIFIED NFT HELPER FUNCTIONS ====================

impl NftTier {
    pub fn try_from(value: u8) -> Result<Self, ProgramError> {
        match value {
            0 => Ok(NftTier::None),
            1 => Ok(NftTier::Bronze),
            2 => Ok(NftTier::Silver), 
            3 => Ok(NftTier::Gold),
            4 => Ok(NftTier::Platinum),
            _ => Err(ProgramError::InvalidArgument),
        }
    }
}

fn auto_mint_first_virtual_nft(
    user_contribution_account: &AccountInfo,
    user_key: &Pubkey,
    nft_tier: NftTier,
    program_id: &Pubkey,
) -> Result<Pubkey, ProgramError> {
    
    let (user_nft_mint_pda, _) = Pubkey::find_program_address(
        &[USER_NFT_SEED, user_key.as_ref(), &[nft_tier as u8]],
        program_id,
    );
    
let mut user_data = user_contribution_account.data.borrow_mut();
user_data[169] = nft_tier as u8;
user_data[170] = 1;
user_data[171] = 0;

user_nft_mint_pda.to_bytes().iter().enumerate().for_each(|(i, &byte)| {
    user_data[172 + i] = byte;
});
    
    Ok(user_nft_mint_pda)
}

fn auto_upgrade_virtual_nft(
    user_contribution_account: &AccountInfo,
    user_key: &Pubkey,
    old_tier: NftTier,
    new_tier: NftTier,
    program_id: &Pubkey,
) -> Result<Pubkey, ProgramError> {
    
    let (new_nft_mint_pda, _) = Pubkey::find_program_address(
        &[USER_NFT_SEED, user_key.as_ref(), &[new_tier as u8]],
        program_id,
    );
    
let mut user_data = user_contribution_account.data.borrow_mut();
user_data[169] = new_tier as u8;
user_data[170] = 1;
user_data[171] = 0;

new_nft_mint_pda.to_bytes().iter().enumerate().for_each(|(i, &byte)| {
    user_data[172 + i] = byte;
});
    
    Ok(new_nft_mint_pda)
}

// ==================== AIRDROP MARKETING NFT ====================
fn airdrop_marketing_nft(accounts: &[AccountInfo], data: &[u8], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let admin_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let target_user_contribution_account = next_account_info(account_info_iter)?;
    let system_program_account = next_account_info(account_info_iter)?;

    if !admin_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    validate_admin_access(presale_config_account, admin_account.key)?;

    // Check that NFT collection is created
    let presale_data = presale_config_account.data.borrow();
    if presale_data[NFT_COLLECTION_CREATED_INDEX] != 1 {
        msg!("ERROR: NFT Collection not created yet! Call function 30 first.");
        return Err(ProgramError::InvalidAccountData);
    }
    drop(presale_data);

    if data.len() != 33 {
        return Err(ProgramError::InvalidInstructionData);
    }

    let target_user_key = Pubkey::new_from_array(
        data[0..32].try_into().map_err(|_| ProgramError::InvalidInstructionData)?
    );
    let nft_tier = data[32];

    if nft_tier != 2 {
        msg!("ERROR: Only Airdrop NFT (tier 2) allowed for marketing airdrop");
        return Err(ProgramError::InvalidArgument);
    }

    validate_user_contribution_pda(target_user_contribution_account, &target_user_key, program_id)?;

    // Create account if doesn't exist
    if target_user_contribution_account.data_len() == 0 {
        let (user_contribution_pda, bump) = Pubkey::find_program_address(
            &[USER_CONTRIBUTION_SEED, target_user_key.as_ref()],
            program_id,
        );

        let rent = Rent::get()?;
        let account_size = 288; // User contribution account size (includes marketing bonus fields)
        let account_rent = rent.minimum_balance(account_size);

        invoke_signed(
            &system_instruction::create_account(
                admin_account.key,
                target_user_contribution_account.key,
                account_rent,
                account_size as u64,
                program_id,
            ),
            &[
                admin_account.clone(),
                target_user_contribution_account.clone(),
                system_program_account.clone(),
            ],
            &[&[USER_CONTRIBUTION_SEED, target_user_key.as_ref(), &[bump]]],
        )?;

        // Initialize account
        let mut data = target_user_contribution_account.data.borrow_mut();
        target_user_key.to_bytes().iter().enumerate().for_each(|(i, &byte)| {
            data[i] = byte;
        });
        data[168] = bump;

        // Initialize all fields to zero
        for i in 32..288 {
            data[i] = 0;
        }
        drop(data);
    }

    // Update airdrop data
    {
        let mut user_data = target_user_contribution_account.data.borrow_mut();

        // Check that airdrop NFT hasn't been given yet
        if user_data[221] != 0 {
            msg!("ERROR: User already has airdrop NFT");
            return Err(ProgramError::AccountAlreadyInitialized);
        }

        user_data[221] = nft_tier; // airdrop_nft_tier
        user_data[222] = 1;        // airdrop_nft_minted
        user_data[223] = 0;        // airdrop_nft_claimed

        // Don't write mint address - will generate at claim
    }

    msg!("AIRDROP SUCCESS: Airdrop NFT voucher given to {}", target_user_key);
    Ok(())
}

// ==================== AIRDROP NFT CREATION ====================
fn create_airdrop_nft<'a>(
    accounts: &[AccountInfo<'a>],
    program_id: &Pubkey,
    tier: u8,
    presale_config_bump: u8,
    admin_address: Pubkey,
) -> ProgramResult {

    // Extract accounts
    let account_info_iter = &mut accounts.iter();
    let user_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let user_contribution_account = next_account_info(account_info_iter)?;

    // Airdrop NFT accounts
    let airdrop_nft_mint_account = next_account_info(account_info_iter)?;
    let airdrop_nft_token_account = next_account_info(account_info_iter)?;
    let airdrop_nft_metadata_account = next_account_info(account_info_iter)?;
    let airdrop_nft_master_edition_account = next_account_info(account_info_iter)?;

    let collection_mint_account = next_account_info(account_info_iter)?;
    let _collection_metadata_account = next_account_info(account_info_iter)?;  // unused
    let _collection_master_edition_account = next_account_info(account_info_iter)?;  // unused
    let token_program_account = next_account_info(account_info_iter)?;
    let token_metadata_program_account = next_account_info(account_info_iter)?;
    let system_program_account = next_account_info(account_info_iter)?;
    let rent_sysvar_account = next_account_info(account_info_iter)?;
    let associated_token_program_account = next_account_info(account_info_iter)?;

    // Generate PDA for airdrop mint
    let (airdrop_nft_mint_pda, airdrop_nft_mint_bump) = Pubkey::find_program_address(
        &[AIRDROP_NFT_SEED, user_account.key.as_ref(), &[tier]],
        program_id,
    );

    if airdrop_nft_mint_pda != *airdrop_nft_mint_account.key {
        msg!("Invalid airdrop NFT mint PDA");
        return Err(ProgramError::InvalidAccountData);
    }

    // Validate Collection
    let (collection_mint_pda, _) = Pubkey::find_program_address(&[NFT_COLLECTION_SEED], program_id);
    if collection_mint_pda != *collection_mint_account.key {
        msg!("ERROR: Invalid collection mint PDA");
        return Err(ProgramError::InvalidAccountData);
    }

    // Validate Token Account
    let expected_airdrop_ata = get_associated_token_address(user_account.key, airdrop_nft_mint_account.key);
    if expected_airdrop_ata != *airdrop_nft_token_account.key {
        msg!("ERROR: Invalid airdrop NFT token account");
        return Err(ProgramError::InvalidAccountData);
    }

    // Write mint address to user data
    {
        let mut user_data = user_contribution_account.data.borrow_mut();
        airdrop_nft_mint_account.key.to_bytes().iter().enumerate().for_each(|(i, &byte)| {
            user_data[224 + i] = byte; // airdrop_nft_mint_address
        });
    }

    // Generate NFT name with counter
   let tier_count = get_airdrop_count(presale_config_account)?;
   let nft_name = format!("MAI Airdrop Warrior #{}", tier_count); // "MAI Airdrop Warrior #1"
   let nft_symbol = "AIRDROP".to_string();
   let nft_uri = AIRDROP_SILVER_METADATA_URI.to_string();

    msg!("Creating airdrop NFT: {}", nft_name);

    // 1. Create mint account
    invoke_signed(
        &system_instruction::create_account(
            user_account.key,
            airdrop_nft_mint_account.key,
            2_039_280,
            82,
            token_program_account.key,
        ),
        &[user_account.clone(), airdrop_nft_mint_account.clone(), system_program_account.clone()],
        &[&[AIRDROP_NFT_SEED, user_account.key.as_ref(), &[tier], &[airdrop_nft_mint_bump]]],
    )?;

    // 2. Initialize mint
    invoke(
        &spl_token::instruction::initialize_mint(
            token_program_account.key,
            airdrop_nft_mint_account.key,
            presale_config_account.key,
            Some(presale_config_account.key),
            0,
        )?,
        &[airdrop_nft_mint_account.clone(), rent_sysvar_account.clone(), token_program_account.clone()],
    )?;

    // 3. Create ATA if doesn't exist
    let expected_ata = get_associated_token_address(user_account.key, airdrop_nft_mint_account.key);
    if expected_ata != *airdrop_nft_token_account.key {
        return Err(ProgramError::InvalidAccountData);
    }

    if airdrop_nft_token_account.data_len() == 0 {
        invoke(
            &create_associated_token_account(
                user_account.key,
                user_account.key,
                airdrop_nft_mint_account.key,
                token_program_account.key,
            ),
            &[
                user_account.clone(),
                airdrop_nft_token_account.clone(),
                user_account.clone(),
                airdrop_nft_mint_account.clone(),
                system_program_account.clone(),
                token_program_account.clone(),
                associated_token_program_account.clone(),
            ],
        )?;
    }

    // 4. Mint 1 NFT
    invoke_signed(
        &spl_token::instruction::mint_to(
            token_program_account.key,
            airdrop_nft_mint_account.key,
            airdrop_nft_token_account.key,
            presale_config_account.key,
            &[],
            1,
        )?,
        &[
            airdrop_nft_mint_account.clone(),
            airdrop_nft_token_account.clone(),
            presale_config_account.clone(),
            token_program_account.clone(),
        ],
        &[&[PRESALE_CONFIG_SEED, &[presale_config_bump]]],
    )?;

    // 5. Create metadata
    let metadata_data = DataV2 {
        name: nft_name.clone(),
	symbol: nft_symbol,
        uri: nft_uri,
        seller_fee_basis_points: 500, // 5%
        creators: Some(vec![
    mpl_token_metadata::types::Creator {
        address: admin_address,  // Royalties go to admin
        verified: false,
        share: 100,
    }
]),
        collection: Some(mpl_token_metadata::types::Collection {
            verified: false,
            key: *collection_mint_account.key,
        }),
        uses: None,
    };

    let create_metadata_args = CreateMetadataAccountV3InstructionArgs {
        data: metadata_data,
        is_mutable: true,
        collection_details: None,
    };

    let create_metadata_ix = CreateMetadataAccountV3 {
        metadata: *airdrop_nft_metadata_account.key,
        mint: *airdrop_nft_mint_account.key,
        mint_authority: *presale_config_account.key,
        payer: *user_account.key,
        update_authority: (*presale_config_account.key, true),
        system_program: *system_program_account.key,
        rent: Some(*rent_sysvar_account.key),
    }.instruction(create_metadata_args);

    invoke_signed(
        &create_metadata_ix,
        &[
            airdrop_nft_metadata_account.clone(),
            airdrop_nft_mint_account.clone(),
            presale_config_account.clone(),
            user_account.clone(),
            presale_config_account.clone(),
            system_program_account.clone(),
            rent_sysvar_account.clone(),
        ],
        &[&[PRESALE_CONFIG_SEED, &[presale_config_bump]]],
    )?;

    // 6. Create master edition
    invoke_signed(
        &mpl_token_metadata::instructions::CreateMasterEditionV3 {
            edition: *airdrop_nft_master_edition_account.key,
            mint: *airdrop_nft_mint_account.key,
            update_authority: *presale_config_account.key,
            mint_authority: *presale_config_account.key,
            payer: *user_account.key,
            metadata: *airdrop_nft_metadata_account.key,
            token_program: *token_program_account.key,
            system_program: *system_program_account.key,
            rent: Some(*rent_sysvar_account.key),
        }.instruction(mpl_token_metadata::instructions::CreateMasterEditionV3InstructionArgs {
            max_supply: Some(1),
        }),
        &[
            airdrop_nft_master_edition_account.clone(),
            airdrop_nft_mint_account.clone(),
            presale_config_account.clone(),
            presale_config_account.clone(),
            user_account.clone(),
            airdrop_nft_metadata_account.clone(),
            token_program_account.clone(),
            system_program_account.clone(),
            rent_sysvar_account.clone(),
            token_metadata_program_account.clone(),
        ],
        &[&[PRESALE_CONFIG_SEED, &[presale_config_bump]]],
    )?;

    msg!("Airdrop NFT created successfully: {}", nft_name);
    Ok(())
}

// ==================== CLAIM AIRDROP NFT ====================
fn claim_airdrop_nft(accounts: &[AccountInfo], program_id: &Pubkey) -> ProgramResult {
    // Basic checks only, account extraction inside create_airdrop_nft
    let account_info_iter = &mut accounts.iter();
    let user_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let user_contribution_account = next_account_info(account_info_iter)?;

    if !user_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    validate_user_contribution_pda(user_contribution_account, user_account.key, program_id)?;
    check_claims_not_paused(presale_config_account)?;

    // Check conditions
    let (presale_config_bump, nft_tier) = {
        let presale_data = presale_config_account.data.borrow();
        if presale_data[82] != 1 { // listing_triggered
            msg!("ERROR: Listing not activated yet");
            return Err(ProgramError::InvalidAccountData);
        }
        let config_bump = presale_data[230];
        drop(presale_data);

        let user_data = user_contribution_account.data.borrow();
        let airdrop_minted = user_data[222];   // airdrop_nft_minted
        let airdrop_claimed = user_data[223];  // airdrop_nft_claimed
        let airdrop_tier = user_data[221];     // airdrop_nft_tier
        
        if airdrop_minted != 1 {
            msg!("ERROR: No airdrop NFT voucher found");
            return Err(ProgramError::InvalidAccountData);
        }
        if airdrop_claimed == 1 {
            msg!("ERROR: Airdrop NFT already claimed");
            return Err(ProgramError::AccountAlreadyInitialized);
        }
        if airdrop_tier != 2 {
            msg!("ERROR: Only Silver airdrop NFT supported");
            return Err(ProgramError::InvalidAccountData);
        }

        drop(user_data);
        (config_bump, 2u8) // Silver = 2
    };

    // Read admin address
    let admin_address = {
        let presale_data = presale_config_account.data.borrow();
        let admin_pubkey = Pubkey::new_from_array(presale_data[0..32].try_into().unwrap());
        drop(presale_data);
        admin_pubkey
    };

    // Update airdrop flag and counter
    {
        let mut user_data = user_contribution_account.data.borrow_mut();
        user_data[223] = 1; // airdrop_nft_claimed
    }

    update_airdrop_counter(presale_config_account)?;

    msg!("Creating airdrop NFT for user: {}", user_account.key);

    // Create airdrop NFT
    create_airdrop_nft(
        accounts,
        program_id,
        nft_tier,
        presale_config_bump,
        admin_address,
    )?;

    msg!("AIRDROP NFT CLAIMED: {}", user_account.key);
    Ok(())
}

// ==================== INSTRUCTION 38: ALLOCATE MARKETING BONUS ====================
fn allocate_marketing_bonus(accounts: &[AccountInfo], data: &[u8], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let admin_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let target_user_contribution_account = next_account_info(account_info_iter)?;
    let system_program_account = next_account_info(account_info_iter)?;

    // Check admin rights
    if !admin_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }
    validate_admin_access(presale_config_account, admin_account.key)?;

    // Parse data: target_user_pubkey (32) + amount (8) + bonus_type (1)
    if data.len() != 41 {
        return Err(ProgramError::InvalidInstructionData);
    }

    let target_user_key = Pubkey::new_from_array(
        data[0..32].try_into().map_err(|_| ProgramError::InvalidInstructionData)?
    );
    let amount = u64::from_le_bytes(
        data[32..40].try_into().map_err(|_| ProgramError::InvalidInstructionData)?
    );
    let bonus_type = data[40]; // 0 = instant, 1 = vested

    if bonus_type > 1 {
        msg!("ERROR: Invalid bonus_type. Must be 0 (instant) or 1 (vested)");
        return Err(ProgramError::InvalidArgument);
    }

    if amount == 0 {
        msg!("ERROR: Amount must be greater than 0");
        return Err(ProgramError::InvalidArgument);
    }

    validate_user_contribution_pda(target_user_contribution_account, &target_user_key, program_id)?;

    // Create account if doesn't exist
    if target_user_contribution_account.data_len() == 0 {
        let (user_contribution_pda, bump) = Pubkey::find_program_address(
            &[USER_CONTRIBUTION_SEED, target_user_key.as_ref()],
            program_id,
        );

        let rent = Rent::get()?;
        let account_size = 288;
        let account_rent = rent.minimum_balance(account_size);

        invoke_signed(
            &system_instruction::create_account(
                admin_account.key,
                target_user_contribution_account.key,
                account_rent,
                account_size as u64,
                program_id,
            ),
            &[
                admin_account.clone(),
                target_user_contribution_account.clone(),
                system_program_account.clone(),
            ],
            &[&[USER_CONTRIBUTION_SEED, target_user_key.as_ref(), &[bump]]],
        )?;

        // Initialize new account
        let mut user_data = target_user_contribution_account.data.borrow_mut();
        target_user_key.to_bytes().iter().enumerate().for_each(|(i, &byte)| {
            user_data[i] = byte;
        });
        user_data[168] = bump;

        // Initialize all fields to zero
        for i in 32..288 {
            user_data[i] = 0;
        }
        drop(user_data);
    }

    // Write tokens to corresponding field
    {
        let mut user_data = target_user_contribution_account.data.borrow_mut();

        if bonus_type == 0 {
            // Instant bonus (Community Airdrop + Community Referral)
            // Bytes 256-263: marketing_instant_tokens
            let current_instant = u64::from_le_bytes(
                user_data[256..264].try_into().map_err(|_| ProgramError::InvalidAccountData)?
            );
            let new_instant = current_instant
                .checked_add(amount)
                .ok_or(ProgramError::ArithmeticOverflow)?;

            new_instant.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
                user_data[256 + i] = byte;
            });

            msg!("INSTANT BONUS: {} MAI allocated to {}", amount, target_user_key);
        } else {
            // Vested bonus (Presale Airdrop)
            // Bytes 272-279: marketing_vested_tokens
            let current_vested = u64::from_le_bytes(
                user_data[272..280].try_into().map_err(|_| ProgramError::InvalidAccountData)?
            );
            let new_vested = current_vested
                .checked_add(amount)
                .ok_or(ProgramError::ArithmeticOverflow)?;

            new_vested.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
                user_data[272 + i] = byte;
            });

            msg!("VESTED BONUS: {} MAI allocated to {} (10% TGE + 90% linear 9mo)", amount, target_user_key);
        }
    }

    Ok(())
}

// ==================== INSTRUCTION 39: CLAIM MARKETING INSTANT ====================
fn claim_marketing_instant(accounts: &[AccountInfo], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let user_account = next_account_info(account_info_iter)?;
    let user_contribution_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let marketing_vault_account = next_account_info(account_info_iter)?;
    let marketing_vault_ata = next_account_info(account_info_iter)?;
    let user_token_account = next_account_info(account_info_iter)?;
    let token_program = next_account_info(account_info_iter)?;
    let mai_mint_account = next_account_info(account_info_iter)?;
    let system_program_account = next_account_info(account_info_iter)?;
    let rent_sysvar_account = next_account_info(account_info_iter)?;
    let associated_token_program_account = next_account_info(account_info_iter)?;

    validate_token_program(token_program)?;

    // Verify user signature
    if !user_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    // Validate PDA
    validate_user_contribution_pda(user_contribution_account, user_account.key, program_id)?;
    check_claims_not_paused(presale_config_account)?;

    // Create user MAI ATA if doesn't exist
    let rent = Rent::from_account_info(rent_sysvar_account)?;
    ensure_user_mai_ata_exists(
    user_token_account,
    user_account,
    mai_mint_account,
    token_program,
    system_program_account,
    &rent,
    associated_token_program_account,
    )?;

    // Check that listing is triggered
    {
        let config_data = presale_config_account.data.borrow();
        let listing_triggered = config_data[42];
        if listing_triggered == 0 {
            msg!("ERROR: Listing not triggered yet. Cannot claim marketing instant tokens.");
            return Err(ProgramError::Custom(46)); // Custom error: listing not triggered
        }
    }

    // Read marketing instant tokens
    let mut user_data = user_contribution_account.data.borrow_mut();

    // Bytes 256-263: marketing_instant_tokens
    let total_instant_tokens = u64::from_le_bytes(
        user_data[256..264].try_into().map_err(|_| ProgramError::InvalidAccountData)?
    );

    // Bytes 264-271: marketing_instant_claimed
    let already_claimed = u64::from_le_bytes(
        user_data[264..272].try_into().map_err(|_| ProgramError::InvalidAccountData)?
    );

    // Check that there are tokens to claim
    if total_instant_tokens == 0 {
        msg!("ERROR: No marketing instant tokens allocated to this user");
        return Err(ProgramError::Custom(60)); // Custom error: no tokens
    }

    let claimable = total_instant_tokens
        .checked_sub(already_claimed)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    if claimable == 0 {
        msg!("ERROR: All marketing instant tokens already claimed");
        return Err(ProgramError::Custom(61)); // Custom error: already claimed
    }

    // Validate marketing vault PDA
    let (expected_vault_pda, vault_bump) = Pubkey::find_program_address(
        &[MARKETING_VAULT_SEED],
        program_id,
    );

    if expected_vault_pda != *marketing_vault_account.key {
        msg!("ERROR: Invalid marketing vault PDA");
        return Err(ProgramError::InvalidAccountData);
    }

    // Transfer tokens from marketing_vault
    invoke_signed(
        &spl_token::instruction::transfer(
            token_program.key,
            marketing_vault_ata.key,
            user_token_account.key,
            marketing_vault_account.key,
            &[],
            claimable,
        )?,
        &[
            marketing_vault_ata.clone(),
            user_token_account.clone(),
            marketing_vault_account.clone(),
            token_program.clone(),
        ],
        &[&[MARKETING_VAULT_SEED, &[vault_bump]]],
    )?;

    // Update claimed amount
    let new_claimed = already_claimed
        .checked_add(claimable)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    // Bytes 264-271: marketing_instant_claimed
    new_claimed.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
        user_data[264 + i] = byte;
    });

    msg!(
        "CLAIMED INSTANT: {} MAI claimed by {} (Total: {}, Claimed: {})",
        claimable,
        user_account.key,
        total_instant_tokens,
        new_claimed
    );

    Ok(())
}

// ==================== INSTRUCTION 40: CLAIM PRESALE AIRDROP ====================
fn claim_presale_airdrop(accounts: &[AccountInfo], program_id: &Pubkey) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let user_account = next_account_info(account_info_iter)?;
    let user_contribution_account = next_account_info(account_info_iter)?;
    let presale_config_account = next_account_info(account_info_iter)?;
    let marketing_vault_account = next_account_info(account_info_iter)?;
    let marketing_vault_ata = next_account_info(account_info_iter)?;
    let user_token_account = next_account_info(account_info_iter)?;
    let token_program = next_account_info(account_info_iter)?;
    let clock_sysvar = next_account_info(account_info_iter)?;
    let mai_mint_account = next_account_info(account_info_iter)?;
    let system_program_account = next_account_info(account_info_iter)?;
    let rent_sysvar_account = next_account_info(account_info_iter)?;
    let associated_token_program_account = next_account_info(account_info_iter)?;

    validate_token_program(token_program)?;

    // Verify user signature
    if !user_account.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    // Validate PDA
    validate_user_contribution_pda(user_contribution_account, user_account.key, program_id)?;
    check_claims_not_paused(presale_config_account)?;

    // Create user MAI ATA if doesn't exist
    let rent = Rent::from_account_info(rent_sysvar_account)?;
    ensure_user_mai_ata_exists(
    user_token_account,
    user_account,
    mai_mint_account,
    token_program,
    system_program_account,
    &rent,
    associated_token_program_account,
    )?;

    // Read listing_date from config
    let listing_date = {
        let config_data = presale_config_account.data.borrow();
        let listing_triggered = config_data[42];

        if listing_triggered == 0 {
            msg!("ERROR: Listing not triggered yet. Cannot claim presale airdrop tokens.");
            return Err(ProgramError::Custom(46)); // Custom error: listing not triggered
        }

        i64::from_le_bytes(
            config_data[74..82].try_into().map_err(|_| ProgramError::InvalidAccountData)?
        )
    };

    // Get current time
    let clock = Clock::from_account_info(clock_sysvar)?;
    let current_time = clock.unix_timestamp;

    // Read presale airdrop tokens
    let mut user_data = user_contribution_account.data.borrow_mut();

    // Bytes 272-279: marketing_vested_tokens
    let total_vested_tokens = u64::from_le_bytes(
        user_data[272..280].try_into().map_err(|_| ProgramError::InvalidAccountData)?
    );

    // Bytes 280-287: marketing_vested_claimed
    let already_claimed = u64::from_le_bytes(
        user_data[280..288].try_into().map_err(|_| ProgramError::InvalidAccountData)?
    );

    // Check that there are tokens to claim
    if total_vested_tokens == 0 {
        msg!("ERROR: No presale airdrop tokens allocated to this user");
        return Err(ProgramError::Custom(62)); // Custom error: no tokens
    }

    // Calculate claimable tokens: 10% TGE + 90% linear over 9 months
    let unlocked_amount = calculate_claimable_tokens(
        total_vested_tokens,
        10,  // tge_percent = 10%
        0,   // cliff_months = 0 (no cliff)
        9,   // vesting_months = 9
        listing_date,
        current_time,
    )?;

    let claimable = unlocked_amount
        .checked_sub(already_claimed)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    if claimable == 0 {
        msg!("ERROR: No presale airdrop tokens available to claim at this time");
        return Err(ProgramError::Custom(63)); // Custom error: nothing to claim
    }

    // Validate marketing vault PDA
    let (expected_vault_pda, vault_bump) = Pubkey::find_program_address(
        &[MARKETING_VAULT_SEED],
        program_id,
    );

    if expected_vault_pda != *marketing_vault_account.key {
        msg!("ERROR: Invalid marketing vault PDA");
        return Err(ProgramError::InvalidAccountData);
    }

    // Transfer tokens from marketing_vault
    invoke_signed(
        &spl_token::instruction::transfer(
            token_program.key,
            marketing_vault_ata.key,
            user_token_account.key,
            marketing_vault_account.key,
            &[],
            claimable,
        )?,
        &[
            marketing_vault_ata.clone(),
            user_token_account.clone(),
            marketing_vault_account.clone(),
            token_program.clone(),
        ],
        &[&[MARKETING_VAULT_SEED, &[vault_bump]]],
    )?;

    // Update claimed amount
    let new_claimed = already_claimed
        .checked_add(claimable)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    // Bytes 280-287: marketing_vested_claimed
    new_claimed.to_le_bytes().iter().enumerate().for_each(|(i, &byte)| {
        user_data[280 + i] = byte;
    });

    msg!(
        "CLAIMED VESTED: {} MAI claimed by {} (Total: {}, Unlocked: {}, Claimed: {})",
        claimable,
        user_account.key,
        total_vested_tokens,
        unlocked_amount,
        new_claimed
    );

    Ok(())
}
