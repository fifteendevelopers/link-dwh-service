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
        Schema::table('Dim_Delivery_Header', function (Blueprint $table) {
            // Integration Reference Key link
            $table->unsignedBigInteger('External_System_Key')->nullable()->after('Training_Provider_Key');

            // Operational Note Columns & Venue text descriptions
            $table->tinyInteger('Pref_Alt_Delivery_Location')->default(0)->after('Consent_Cutoff_Date');
            $table->text('Alt_Delivery_Location')->nullable()->after('Pref_Alt_Delivery_Location');
            $table->text('Notes')->nullable()->after('Alt_Delivery_Location');
            $table->text('Instructor_General_Notes')->nullable()->after('Notes');
            $table->text('Teacher_Notes')->nullable()->after('Instructor_General_Notes');

            // Structured JSON Blobs from Source System longtexts
            $table->json('School_Contacts')->nullable()->after('Teacher_Notes');
            $table->json('Venue')->nullable()->after('School_Contacts');
            $table->json('Provider_Additional_Questions')->nullable()->after('Venue');

            // Operational Configuration Rules & Date metrics
            $table->date('Comms_Start_Date')->nullable()->after('Provider_Additional_Questions');
            $table->date('Date_Completed')->nullable()->after('Comms_Start_Date');
            $table->tinyInteger('Pref_Link_Managed_Consent')->nullable()->after('Date_Completed');
            $table->tinyInteger('Include_Tp_Terms_In_Consent')->default(0)->after('Pref_Link_Managed_Consent');
            $table->tinyInteger('Consent_Src_Characteristics')->default(0)->after('Include_Tp_Terms_In_Consent');
            $table->integer('Max_Consents')->nullable()->after('Consent_Src_Characteristics');
            $table->tinyInteger('Waiting_List_Enabled')->default(0)->after('Max_Consents');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('Dim_Delivery_Header', function (Blueprint $table) {
            $table->dropColumn([
                'External_System_Key',
                'Pref_Alt_Delivery_Location',
                'Alt_Delivery_Location',
                'Notes',
                'Instructor_General_Notes',
                'Teacher_Notes',
                'School_Contacts',
                'Venue',
                'Provider_Additional_Questions',
                'Comms_Start_Date',
                'Date_Completed',
                'Pref_Link_Managed_Consent',
                'Include_Tp_Terms_In_Consent',
                'Consent_Src_Characteristics',
                'Max_Consents',
                'Waiting_List_Enabled'
            ]);
        });
    }
};
