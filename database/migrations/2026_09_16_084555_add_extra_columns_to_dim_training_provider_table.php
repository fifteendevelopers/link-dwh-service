<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     */
    public function up(): void
    {
        Schema::table('Dim_Training_Provider', function (Blueprint $table) {
            // Contact & Emails
            $table->string('Primary_Email')->nullable()->after('Telephone');
            $table->string('Secondary_Email')->nullable()->after('Primary_Email');
            $table->string('Landline')->nullable()->after('Public_Telephone');

            // Status & Lifecycle Dates
            $table->tinyInteger('Status')->default(0)->after('Provider_Type');
            $table->date('Date_Inception')->nullable()->after('Status');
            $table->date('Date_Renewal')->nullable()->after('Date_Inception');
            $table->date('Date_Deregistered')->nullable()->after('Date_Renewal');
            $table->text('Deregistration_Reason')->nullable()->after('Date_Deregistered');
            $table->date('Date_Eqa_Visit')->nullable()->after('Deregistration_Reason');
            $table->date('Date_Insurance_Expiry')->nullable()->after('Date_Eqa_Visit');
            $table->boolean('Renewal_Blocked')->default(false)->after('Date_Insurance_Expiry');

            // Module Delivery Preferences (Course Levels)
            $table->boolean('Pref_Level_1')->default(false)->after('Renewal_Blocked');
            $table->boolean('Pref_Level_2')->default(false)->after('Pref_Level_1');
            $table->boolean('Pref_Level_3')->default(false)->after('Pref_Level_2');
            $table->boolean('Pref_Plus_Balance')->default(false)->after('Pref_Level_3');
            $table->boolean('Pref_Plus_Bus')->default(false)->after('Pref_Plus_Balance');
            $table->boolean('Pref_Plus_Fix')->default(false)->after('Pref_Plus_Bus');
            $table->boolean('Pref_Plus_Learn')->default(false)->after('Pref_Plus_Fix');
            $table->boolean('Pref_Plus_On_Show')->default(false)->after('Pref_Plus_Learn');
            $table->boolean('Pref_Plus_Parents')->default(false)->after('Pref_Plus_On_Show');
            $table->boolean('Pref_Plus_Promotion')->default(false)->after('Pref_Plus_Parents');
            $table->boolean('Pref_Plus_Recycled')->default(false)->after('Pref_Plus_Promotion');
            $table->boolean('Pref_Plus_Ride')->default(false)->after('Pref_Plus_Recycled');
            $table->boolean('Pref_Plus_Transition')->default(false)->after('Pref_Plus_Ride');
            $table->boolean('Pref_Plus_Family')->default(false)->after('Pref_Plus_Transition');
            $table->boolean('Pref_Plus_Adult')->default(false)->after('Pref_Plus_Family');

            // Operational Details & Custom Configuration
            $table->longText('Legacy_Areas_Of_Operation')->nullable()->after('Pref_Plus_Adult');
            $table->longText('Delivery_Areas')->nullable()->after('Legacy_Areas_Of_Operation');
            $table->text('Account_Notes')->nullable()->after('Delivery_Areas');
            $table->string('Terms_Url', 2000)->nullable()->after('Account_Notes');

            // Custom Booking Questions
            $table->text('Booking_Question_1')->nullable()->after('Terms_Url');
            $table->text('Booking_Question_2')->nullable()->after('Booking_Question_1');
            $table->text('Booking_Question_3')->nullable()->after('Booking_Question_2');
            $table->text('Booking_Question_4')->nullable()->after('Booking_Question_3');
            $table->text('Booking_Question_5')->nullable()->after('Booking_Question_4');

            // Operational Consent & Fleet Preferences
            $table->boolean('Pref_Collect_Characteristics_In_Consent')->default(false)->after('Booking_Question_5');
            $table->boolean('Pref_Allow_Non_Riders')->default(false)->after('Pref_Collect_Characteristics_In_Consent');
            $table->boolean('Pref_Enable_Tp_Digi_Delivery_Access')->default(false)->after('Pref_Allow_Non_Riders');
            $table->boolean('Pref_Enable_Tp_Digi_Consent_Upload')->default(false)->after('Pref_Enable_Tp_Digi_Delivery_Access');
            $table->boolean('Has_Fleet_Cycles')->default(false)->after('Pref_Enable_Tp_Digi_Consent_Upload');
            $table->boolean('Provide_A_Cycle_Question_Optional')->default(false)->after('Has_Fleet_Cycles');
            $table->unsignedBigInteger('External_System_Id')->nullable()->after('Provide_A_Cycle_Question_Optional');

            // Source Deleted At
            $table->timestamp('Source_Deleted_At')->nullable()->after('Source_Updated_At');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('Dim_Training_Provider', function (Blueprint $table) {
            $table->dropColumn([
                'Primary_Email',
                'Secondary_Email',
                'Landline',
                'Status',
                'Date_Inception',
                'Date_Renewal',
                'Date_Deregistered',
                'Deregistration_Reason',
                'Date_Eqa_Visit',
                'Date_Insurance_Expiry',
                'Renewal_Blocked',
                'Pref_Level_1',
                'Pref_Level_2',
                'Pref_Level_3',
                'Pref_Plus_Balance',
                'Pref_Plus_Bus',
                'Pref_Plus_Fix',
                'Pref_Plus_Learn',
                'Pref_Plus_On_Show',
                'Pref_Plus_Parents',
                'Pref_Plus_Promotion',
                'Pref_Plus_Recycled',
                'Pref_Plus_Ride',
                'Pref_Plus_Transition',
                'Pref_Plus_Family',
                'Pref_Plus_Adult',
                'Legacy_Areas_Of_Operation',
                'Delivery_Areas',
                'Account_Notes',
                'Terms_Url',
                'Booking_Question_1',
                'Booking_Question_2',
                'Booking_Question_3',
                'Booking_Question_4',
                'Booking_Question_5',
                'Pref_Collect_Characteristics_In_Consent',
                'Pref_Allow_Non_Riders',
                'Pref_Enable_Tp_Digi_Delivery_Access',
                'Pref_Enable_Tp_Digi_Consent_Upload',
                'Has_Fleet_Cycles',
                'Provide_A_Cycle_Question_Optional',
                'External_System_Id',
                'Source_Deleted_At',
            ]);
        });
    }
};
